"""Pydantic models for the football single-table design.

Each model knows how to serialize itself into a DynamoDB item (`to_item`)
and how to be rebuilt from one (`from_item`). Items are plain dicts as
returned by the boto3 *resource* API (already deserialized from the
low-level DynamoDB types).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from app.models import keys
from app.models.keys import EventType

__all__ = ["EventType", "Competition", "Game", "GameEvent", "Team", "Player", "GameTeamIndex"]


class Competition(BaseModel):
    competition_name: str
    country: str  # ISO 3166-2, e.g. "DE"
    current_season: str
    # Defaults to football so existing behavior/keys are unchanged; other
    # sports (e.g. basketball) pass keys.SPORT_BASKETBALL explicitly.
    sport: str = keys.SPORT_FOOTBALL

    def to_item(self) -> dict[str, Any]:
        return {
            "PK": keys.sport_pk(self.sport, self.competition_name),
            "SK": keys.competition_metadata_sk(),
            "EntityType": "COMPETITION",
            "Sport": self.sport,
            "CompetitionName": self.competition_name,
            "Country": self.country,
            "CurrentSeason": self.current_season,
        }

    @classmethod
    def from_item(cls, item: dict[str, Any]) -> "Competition":
        return cls(
            competition_name=item["CompetitionName"],
            country=item["Country"],
            current_season=item["CurrentSeason"],
            sport=item.get("Sport", keys.SPORT_FOOTBALL),
        )


class Game(BaseModel):
    competition_name: str
    game_id: str
    kickoff: str
    team_name_1: str
    team_name_2: str
    game_date: str
    score: str = "0-0"
    sport: str = keys.SPORT_FOOTBALL

    def to_item(self) -> dict[str, Any]:
        return {
            "PK": keys.sport_pk(self.sport, self.competition_name),
            "SK": keys.game_metadata_sk(self.game_id),
            "EntityType": "GAME",
            "Sport": self.sport,
            "GameId": self.game_id,
            "KickOff": self.kickoff,
            "TeamName1": self.team_name_1,
            "TeamName2": self.team_name_2,
            "GameDate": self.game_date,
            "Score": self.score,
        }

    @classmethod
    def from_item(cls, item: dict[str, Any], competition_name: str) -> "Game":
        return cls(
            competition_name=competition_name,
            game_id=item["GameId"],
            kickoff=item["KickOff"],
            team_name_1=item["TeamName1"],
            team_name_2=item["TeamName2"],
            game_date=item["GameDate"],
            score=item.get("Score", "0-0"),
            sport=item.get("Sport", keys.SPORT_FOOTBALL),
        )


class GameEvent(BaseModel):
    competition_name: str
    game_id: str
    event_type: EventType
    player: str
    team: str
    minute: int
    event_id: uuid.UUID = Field(default_factory=keys.uuid7)
    # Latency-tracking timestamps, propagated end-to-end through Kafka so
    # every pipeline stage can measure its own latency contribution against
    # the same origin points (see README "Measuring Latency").
    #   provider_emitted_at: when the provider says the event happened
    #     (parsed from the provider's own timestamp field). Approximates t0.
    #   ingested_at: when our normalization layer processed the raw payload
    #     (t2) -- i.e. as early as possible after our system saw the event.
    provider_emitted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    sport: str = keys.SPORT_FOOTBALL

    def to_item(self) -> dict[str, Any]:
        return {
            "PK": keys.sport_pk(self.sport, self.competition_name),
            "SK": keys.game_event_sk(self.game_id, self.event_type, self.event_id),
            "EntityType": f"EVENT#{self.event_type.value}",
            "Sport": self.sport,
            "GameId": self.game_id,
            "EventType": self.event_type.value,
            "Player": self.player,
            "Team": self.team,
            "Minute": self.minute,
            "ProviderEmittedAt": self.provider_emitted_at.isoformat(),
            "IngestedAt": self.ingested_at.isoformat(),
            # GSI1: enables querying events by followed team.
            "GSI1PK": keys.gsi1_team_pk(self.team),
            "GSI1SK": keys.gsi1_event_sk(self.event_type, self.event_id),
            # GSI2: enables querying a game's events in chronological (by
            # ingestion time) order across all event types, e.g. for a
            # "top N most recent" feed without fetching every event.
            "GSI2PK": keys.gsi2_game_pk(self.competition_name, self.game_id),
            "GSI2SK": keys.gsi2_event_sk(self.ingested_at.isoformat(), self.event_id),
        }

    @classmethod
    def from_item(cls, item: dict[str, Any], competition_name: str) -> "GameEvent":
        # The event's UUIDv7 is the trailing component of the SK
        # (GAMEID#{game_id}#EVENT#{event_type}#{event_id}); parse it back out
        # so reads round-trip the same identity that was written.
        event_id = uuid.UUID(item["SK"].rsplit("#", 1)[-1])
        return cls(
            competition_name=competition_name,
            game_id=item["GameId"],
            event_type=EventType(item["EventType"]),
            player=item["Player"],
            team=item["Team"],
            minute=int(item["Minute"]),
            event_id=event_id,
            provider_emitted_at=datetime.fromisoformat(item["ProviderEmittedAt"]),
            ingested_at=datetime.fromisoformat(item["IngestedAt"]),
            sport=item.get("Sport", keys.SPORT_FOOTBALL),
        )


class Team(BaseModel):
    competition_name: str
    team_name: str
    sport: str = keys.SPORT_FOOTBALL

    def to_item(self) -> dict[str, Any]:
        return {
            "PK": keys.sport_pk(self.sport, self.competition_name),
            "SK": keys.team_sk(self.team_name),
            "EntityType": "TEAM",
            "Sport": self.sport,
            "TeamName": self.team_name,
        }

    @classmethod
    def from_item(cls, item: dict[str, Any], competition_name: str) -> "Team":
        return cls(
            competition_name=competition_name,
            team_name=item["TeamName"],
            sport=item.get("Sport", keys.SPORT_FOOTBALL),
        )


class Player(BaseModel):
    competition_name: str
    team_name: str
    player_name: str
    sport: str = keys.SPORT_FOOTBALL

    def to_item(self) -> dict[str, Any]:
        return {
            "PK": keys.sport_pk(self.sport, self.competition_name),
            "SK": keys.player_sk(self.team_name, self.player_name),
            "EntityType": "PLAYER",
            "Sport": self.sport,
            "TeamName": self.team_name,
            "PlayerName": self.player_name,
        }

    @classmethod
    def from_item(cls, item: dict[str, Any], competition_name: str) -> "Player":
        return cls(
            competition_name=competition_name,
            team_name=item["TeamName"],
            player_name=item["PlayerName"],
            sport=item.get("Sport", keys.SPORT_FOOTBALL),
        )


class GameTeamIndex(BaseModel):
    """Adjacency pointer linking a team to a game (follow-team access pattern)."""

    competition_name: str
    team_name: str
    opponent: str
    home_away: str
    game_id: str
    game_date: str
    kickoff: str
    sport: str = keys.SPORT_FOOTBALL

    def to_item(self) -> dict[str, Any]:
        return {
            "PK": keys.sport_pk(self.sport, self.competition_name),
            "SK": keys.team_game_index_sk(self.team_name, self.game_date, self.game_id),
            "EntityType": "GAME_TEAM_INDEX",
            "Sport": self.sport,
            "GameId": self.game_id,
            "Opponent": self.opponent,
            "HomeAway": self.home_away,
            "GameDate": self.game_date,
            "KickOff": self.kickoff,
            # GSI1: enables querying games by followed team.
            "GSI1PK": keys.gsi1_team_pk(self.team_name),
            "GSI1SK": keys.gsi1_game_sk(self.game_date, self.game_id),
        }

    @classmethod
    def from_item(cls, item: dict[str, Any], competition_name: str) -> "GameTeamIndex":
        return cls(
            competition_name=competition_name,
            team_name=item["GSI1PK"].split("#", 1)[1],
            opponent=item["Opponent"],
            home_away=item["HomeAway"],
            game_id=item["GameId"],
            game_date=item["GameDate"],
            kickoff=item["KickOff"],
            sport=item.get("Sport", keys.SPORT_FOOTBALL),
        )
