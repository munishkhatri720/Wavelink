"""
MIT License

Copyright (c) 2019-Current PythonistaGuild, EvieePy

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from __future__ import annotations

import asyncio

try:
    import orjson as json  # type: ignore
except ImportError:
    import json
import logging
from typing import TYPE_CHECKING, Any, Optional
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed
from urllib.parse import urlparse, urlunparse
from . import __version__
from .backoff import Backoff
from .enums import NodeStatus
from .exceptions import AuthorizationFailedException, NodeException
from .payloads import *
from .tracks import Playable


if TYPE_CHECKING:
    from .node import Node
    from .player import Player
    from .types.request import UpdateSessionRequest
    from .types.response import InfoResponse
    from .types.state import PlayerState
    from .types.websocket import TrackExceptionPayload, WebsocketOP


logger: logging.Logger = logging.getLogger(__name__)
LOGGER_TRACK: logging.Logger = logging.getLogger("TrackException")


class Websocket:
    def __init__(self, *, node: Node) -> None:
        self.node = node

        self.backoff: Backoff = Backoff()

        self.socket: Optional[ClientConnection] = None
        self.keep_alive_task: Optional[asyncio.Task[None]] = None

    @property
    def headers(self) -> dict[str, str]:
        assert self.node.client is not None
        assert self.node.client.user is not None

        data = {
            "Authorization": self.node.password,
            "User-Id": str(self.node.client.user.id),
            "Client-Name": f"Wavelink/{__version__}",
        }

        if self.node.session_id:
            data["Session-Id"] = self.node.session_id

        return data

    def is_connected(self) -> bool:
        return self.socket is not None and self.socket.close_code is None

    async def _update_node(self) -> None:
        if self.node._resume_timeout > 0:
            udata: UpdateSessionRequest = {"resuming": True, "timeout": self.node._resume_timeout}
            await self.node._update_session(data=udata)

        info: InfoResponse = await self.node._fetch_info()
        if "spotify" in info["sourceManagers"]:
            self.node._spotify_enabled = True

    async def connect(self) -> None:
        if self.node._status is NodeStatus.CONNECTED:
            # Node was previously connected...
            # We can dispatch an event to say the node was disconnected...
            payload: NodeDisconnectedEventPayload = NodeDisconnectedEventPayload(node=self.node)
            self.dispatch("node_disconnected", payload)

        self.node._status = NodeStatus.CONNECTING

        if self.keep_alive_task:
            try:
                self.keep_alive_task.cancel()
            except Exception as e:
                logger.debug(
                    "Failed to cancel websocket keep alive while connecting. This is most likely not a problem and will not affect websocket connection: '%s'",
                    e,
                )

        retries: int | None = self.node._retries
        heartbeat: float = self.node.heartbeat
        base = self.node.uri.rstrip("/")
        parsed = urlparse(base)
        scheme = "wss" if parsed.scheme == "https" else "ws"
        uri = urlunparse(parsed._replace(scheme=scheme, path=f"{parsed.path}/v4/websocket"))
        github: str = "https://github.com/PythonistaGuild/Wavelink/issues"

        while True:
            try:
                self.socket = await connect(
                    uri, additional_headers=self.headers, ping_interval=heartbeat, ping_timeout=heartbeat * 2
                )
            except Exception as e:
                status_code = getattr(e, "status_code", None) if hasattr(e, "status_code") else None

                if status_code == 401:
                    await self.cleanup()
                    raise AuthorizationFailedException from e
                elif status_code == 404:
                    await self.cleanup()
                    raise NodeException from e
                else:
                    logger.warning(
                        'An unexpected error occurred while connecting %r to Lavalink: "%s"\nIf this error persists or wavelink is unable to reconnect, please see: %s',
                        self.node,
                        e,
                        github,
                    )

            if self.is_connected():
                self.keep_alive_task = asyncio.create_task(self.keep_alive())
                break

            if retries == 0:
                logger.warning(
                    '%r was unable to successfully connect/reconnect to Lavalink after "%s" connection attempt. This Node has exhausted the retry count.',
                    self.node,
                    retries + 1,
                )

                await self.cleanup()
                break

            if retries:
                retries -= 1

            delay: float = self.backoff.calculate()
            logger.info('%r retrying websocket connection in "%s" seconds.', self.node, delay)

            await asyncio.sleep(delay)

    async def keep_alive(self) -> None:
        assert self.socket is not None

        while True:
            try:
                message_data = await self.socket.recv()

                if message_data is None or message_data == "":
                    logger.debug("Received an empty message from Lavalink websocket. Disregarding.")
                    continue

                if isinstance(message_data, bytes):
                    message_data = message_data.decode("utf-8")

                data: WebsocketOP = json.loads(message_data)

                if data["op"] == "ready":
                    resumed: bool = data["resumed"]
                    session_id: str = data["sessionId"]

                    self.node._status = NodeStatus.CONNECTED
                    self.node._session_id = session_id

                    await self._update_node()

                    ready_payload: NodeReadyEventPayload = NodeReadyEventPayload(
                        node=self.node, resumed=resumed, session_id=session_id
                    )
                    self.dispatch("node_ready", ready_payload)

                elif data["op"] == "playerUpdate":
                    playerup: Player | None = self.get_player(data["guildId"])
                    state: PlayerState = data["state"]

                    updatepayload: PlayerUpdateEventPayload = PlayerUpdateEventPayload(player=playerup, state=state)
                    self.dispatch("player_update", updatepayload)

                    if playerup:
                        asyncio.create_task(playerup._update_event(updatepayload))

                elif data["op"] == "stats":
                    statspayload: StatsEventPayload = StatsEventPayload(data=data)
                    self.node._total_player_count = statspayload.players
                    self.dispatch("stats_update", statspayload)

                elif data["op"] == "event":
                    player: Player | None = self.get_player(data["guildId"])

                    if data["type"] == "TrackStartEvent":
                        track: Playable = Playable(data["track"])

                        startpayload: TrackStartEventPayload = TrackStartEventPayload(player=player, track=track)
                        self.dispatch("track_start", startpayload)

                        if player:
                            asyncio.create_task(player._track_start(startpayload))

                    elif data["type"] == "TrackEndEvent":
                        track: Playable = Playable(data["track"])
                        reason: str = data["reason"]

                        if player and reason != "replaced":
                            player._current = None

                        endpayload: TrackEndEventPayload = TrackEndEventPayload(
                            player=player, track=track, reason=reason
                        )
                        self.dispatch("track_end", endpayload)

                        if player:
                            asyncio.create_task(player._auto_play_event(endpayload))

                    elif data["type"] == "TrackExceptionEvent":
                        track: Playable = Playable(data["track"])
                        exception: TrackExceptionPayload = data["exception"]

                        excpayload: TrackExceptionEventPayload = TrackExceptionEventPayload(
                            player=player, track=track, exception=exception
                        )

                        LOGGER_TRACK.error(
                            "A Lavalink TrackException was received on %r for player %r: %s, caused by: %s, with severity: %s",
                            self.node,
                            player,
                            exception.get("message", ""),
                            exception["cause"],
                            exception["severity"],
                        )
                        self.dispatch("track_exception", excpayload)

                    elif data["type"] == "TrackStuckEvent":
                        track: Playable = Playable(data["track"])
                        threshold: int = data["thresholdMs"]

                        stuckpayload: TrackStuckEventPayload = TrackStuckEventPayload(
                            player=player, track=track, threshold=threshold
                        )
                        self.dispatch("track_stuck", stuckpayload)

                    elif data["type"] == "WebSocketClosedEvent":
                        code: int = data["code"]
                        reason: str = data["reason"]
                        by_remote: bool = data["byRemote"]

                        wcpayload: WebsocketClosedEventPayload = WebsocketClosedEventPayload(
                            player=player, code=code, reason=reason, by_remote=by_remote
                        )
                        self.dispatch("websocket_closed", wcpayload)

                        if player:
                            asyncio.create_task(player._disconnected_wait(code, by_remote))

                    else:
                        other_payload: ExtraEventPayload = ExtraEventPayload(node=self.node, player=player, data=data)
                        self.dispatch("extra_event", other_payload)
                else:
                    logger.debug("'Received an unknown OP from Lavalink '%s'. Disregarding.", data["op"])

            except ConnectionClosed as e:
                logger.warning(
                    "WebSocket connection closed unexpectedly for %r. Code: %s, Reason: %s. Remote: %s",
                    self.node,
                    e.code,
                    getattr(e, "reason", "No reason provided"),
                    getattr(e, "was_clean", "Unknown"),
                )
                asyncio.create_task(self.connect())
                break
            except Exception as e:
                logger.error(f"Error processing websocket message: {e}")
                # Continue to next message

    def get_player(self, guild_id: str | int) -> Player | None:
        return self.node.get_player(int(guild_id))

    def dispatch(self, event: str, /, *args: Any, **kwargs: Any) -> None:
        assert self.node.client is not None

        self.node.client.dispatch(f"wavelink_{event}", *args, **kwargs)
        logger.debug("%r dispatched the event 'on_wavelink_%s'", self.node, event)

    async def cleanup(self) -> None:
        if self.keep_alive_task:
            try:
                self.keep_alive_task.cancel()
            except Exception:
                pass

        if self.socket:
            try:
                await self.socket.close()
            except Exception:
                pass

        self.node._status = NodeStatus.DISCONNECTED
        self.node._session_id = None
        self.node._players = {}

        self.node._websocket = None

        payload: NodeDisconnectedEventPayload = NodeDisconnectedEventPayload(node=self.node)
        self.dispatch("node_disconnected", payload)

        logger.debug("Successfully cleaned up the websocket for %r", self.node)
