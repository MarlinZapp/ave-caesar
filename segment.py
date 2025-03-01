from random import randint
import sys, json
from time import sleep
from kafka import KafkaProducer, KafkaConsumer
from event_system import EventSystem
from utils import draw_card


class ScoutSystem(EventSystem):
    def __init__(self, state, next_segments, producer):
        super().__init__(state)
        self.next_segments = next_segments
        self.producer = producer

    def on_trigger(self):
        scout_segment_index = self.state.get("scout_segment_index")
        if scout_segment_index is None:
            raise ValueError("scout segment index is not set")
        scout_path = self.state.get("scout_path")
        if scout_path is None:
            raise ValueError("scout_path is not set")
        scout_path.append(self.next_segments[scout_segment_index])
        self.producer.send(self.next_segments[scout_segment_index], value={
            "event": "scout",
            "request_from": self.state.get("request_from"),
            "player": self.state.get("player"),
            "scout_steps": self.state.get("scout_steps"),
            "scout_path": scout_path,
            "scout_card_index": self.state.get("scout_card_index"),
        })


class Segment:


    def __init__(self, segment_id, next_segments):
        self.segment_id = segment_id
        self.next_segments = next_segments
        self.scout_requests = dict()
        self.consumer = KafkaConsumer(
            segment_id,
            bootstrap_servers=['localhost:29092', 'localhost:39092', 'localhost:49092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:29092', 'localhost:39092', 'localhost:49092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.occupied = False


    def start_scout(self, player, request_from, scout_path, scout_steps, scout_card_index):
        # print(f"Starting scout from {request_from} to search a path of length {player.get('cards')[0]}")
        system = ScoutSystem({
            "player": player,
            "request_from": request_from,
            "scout_steps": scout_steps,
            "scout_card_index": scout_card_index,
            "scout_segment_index": 0,
            "scout_path": scout_path
        }, self.next_segments, self.producer)
        self.scout_requests[self.segment_id] = system
        system.trigger()


    def handle_scout_failure(self, request_from, player, scout_path):
        cards = player.get("cards")
        system = self.scout_requests[request_from]
        if system is None:
            raise ValueError("Could not find scout system for segment")
        segment_index = system.state.get("scout_segment_index")
        card_index = system.state.get("scout_card_index")

        # try next segment
        if segment_index is not None and segment_index < len(self.next_segments) - 1:
            segment_index = segment_index + 1
            system.state["scout_segment_index"] = segment_index
            system.trigger()
        # try next card if scouting started from this segment and all segments have been scouted
        elif request_from == self.segment_id and segment_index == len(self.next_segments) - 1 and card_index < len(cards) - 1:
            segment_index = 0
            system.state["scout_segment_index"] = segment_index
            card_index = card_index + 1
            system.state["scout_card_index"] = card_index
            system.state["scout_steps"] = cards[card_index]
            system.state["scout_path"] = [self.segment_id]
            print(f"Player {player.get('player_id')} is now trying {card_index}. card ({cards[card_index]})")
            system.trigger()
        # Continue scouting from previous segment
        elif request_from != self.segment_id:
            if len(scout_path) > 0:
                last_segment = scout_path.pop()
                self.producer.send(last_segment, value={
                    "event": "scout_result",
                    "request_from": request_from,
                    "result": "failure",
                    "player": player,
                    "scout_path": scout_path
                })


    def move_player_to_this_segment(self, request_from, player, scout_path, scout_card_index):
        self.occupied = True
        self.producer.send(request_from, value={
            "event": "scout_result",
            "request_from": request_from,
            "result": "success",
            "player": player
        })
        cards = player.get("cards")
        for (i, segment) in enumerate(scout_path):
            if i == 0:
                continue
            if segment.startswith("start-and-goal"):
                player["round"] = player.get("round") + 1
                if player.get("round") == 3:
                    print(f"Player {player.get('player_id')} has finished!")
                    return
                else:
                    print(f"Player {player.get('player_id')} finished round {player.get('round')}.")
        # Exchange used card with new card
        old_card = cards.pop(scout_card_index)
        cards.append(draw_card())
        player["cards"] = cards
        print(f"Player {player.get('player_id')} used card {old_card} and drew card {cards[-1]}. New cards: {cards}")
        sleep_time = randint(1, 3)
        print(f"Player {player.get('player_id')} moved from {request_from} to {self.segment_id} using this path: {scout_path}. Waiting {sleep_time} seconds...")
        sleep(sleep_time)
        if len(cards) == 0:
            print(f"Player {player.get('player_id')} has no more cards. He lost.")
            return
        else:
            # Play next card
            self.start_scout(player, self.segment_id, [self.segment_id], cards[0], 0)


    def handle_scout(self, scout_steps, request_from, player, scout_path, scout_card_index):
        if self.occupied and request_from != self.segment_id:
            self.producer.send(request_from, value={
                "event": "scout_result",
                "request_from": request_from,
                "result": "failure",
                "player": player,
            })
            return
        if scout_steps == 1:
            self.move_player_to_this_segment(request_from, player, scout_path, scout_card_index)
        else:
            self.start_scout(player, request_from, scout_path, scout_steps-1, scout_card_index)


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} segment_id next_segment1,next_segment2,...")
        sys.exit(1)
    segment_id = sys.argv[1]
    next_segments = sys.argv[2].split(",")
    segment = Segment(segment_id, next_segments)
    for msg in segment.consumer:
        if msg.value.get("event") == "start":
            player = msg.value.get("player")
            print(f"Player {player.get('player_id')}: Starting scout from {segment_id} to search a path of length {player.get('cards')[0]}")
            segment.start_scout(player, segment_id, [segment_id], player.get("cards")[0], 0)
        elif msg.value.get("event") == "scout_result":
            if msg.value.get("result") == "success":
                segment.occupied = False
            elif segment.scout_requests is not None:
                segment.handle_scout_failure(msg.value.get("request_from"), msg.value.get("player"), msg.value.get("scout_path"))
        elif msg.value.get("event") == "scout":
            segment.handle_scout(
                msg.value.get("scout_steps"),
                msg.value.get("request_from"),
                msg.value.get("player"),
                msg.value.get("scout_path"),
                msg.value.get("scout_card_index")
            )

if __name__ == "__main__":
    main()
