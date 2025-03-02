import sys, json
import threading
from time import sleep
from kafka import KafkaProducer, KafkaConsumer
from utils import draw_card
from dataclasses import dataclass

@dataclass
class ScoutingState:
    scout_segment_index: int
    scout_path: list[str]
    scout_steps: int
    scout_card_index: int
    request_origin: str
    player: dict


class Segment:
    def __init__(self, segment_id : str, segment_type : str, next_segments : list[str]):
        self.segment_id = segment_id
        self.segment_type = segment_type
        self.next_segments = next_segments
        self.scouting_states = dict()
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


    def send_scout_message(self, state: ScoutingState):
        state.scout_path.append(self.next_segments[state.scout_segment_index])
        self.producer.send(self.next_segments[state.scout_segment_index], value={
            "event": "scout",
            "request_origin": state.request_origin,
            "player": state.player,
            "scout_steps": state.scout_steps,
            "scout_path": state.scout_path,
            "scout_card_index": state.scout_card_index,
        })


    def start_scout(self, player, request_origin, scout_path, scout_steps, scout_card_index):
        # print(f"Starting scout from {request_origin} to search a path of length {player.get('cards')[0]}")
        if request_origin == self.segment_id:
            print(f"Player {player.get('player_id')} is trying to play the card {player['cards'][0]}")
        state = ScoutingState(
            player=player,
            request_origin=request_origin,
            scout_segment_index=0,
            scout_path=scout_path,
            scout_steps=scout_steps,
            scout_card_index=scout_card_index,
        )
        self.send_scout_message(state)
        self.scouting_states[request_origin] = state


    def handle_scout_failure(self, request_origin, player):
        cards = player.get("cards")
        state : ScoutingState = self.scouting_states[request_origin]
        if state is None:
            raise ValueError("Could not find scouting state for segment")
        segment_index = state.scout_segment_index
        card_index = state.scout_card_index
        state.scout_path.pop() # Segment that failed
        state.scout_path.pop() # This segment
        previous_segment = None
        if len(state.scout_path) > 0:
            previous_segment = state.scout_path.pop() # Previous segment

        # try next segment
        if segment_index is not None and segment_index < len(self.next_segments) - 1:
            segment_index = segment_index + 1
            state.scout_segment_index = segment_index
            if previous_segment is not None:
                state.scout_path.append(previous_segment)
            state.scout_path.append(self.segment_id)
            self.send_scout_message(state)
        # try next card if scouting started from this segment and all segments have been scouted
        elif request_origin == self.segment_id and segment_index == len(self.next_segments) - 1 and card_index < len(cards) - 1:
            state.scout_segment_index = 0
            state.scout_card_index = card_index + 1
            state.scout_steps = cards[card_index]
            state.scout_path = [self.segment_id]
            print(f"Player {player.get('player_id')} is trying to play the card {cards[card_index]}")
            self.send_scout_message(state)
        # Continue scouting from previous segment or skip turn
        else:
            if previous_segment is not None:
                # print(f"Tried all ways from {self.segment_id}, moving back to {previous_segment}.")
                self.producer.send(previous_segment, value={
                    "event": "scout_result",
                    "request_origin": request_origin,
                    "result": "failure",
                    "player": player,
                })
            else:
                print(f"Player {player.get('player_id')} has no possibility to move. Skipping turn (waiting 5 seconds).")
                sleep(5)
                self.start_scout(player, self.segment_id, [self.segment_id], cards[0], 0)


    def move_player_to_this_segment(self, request_origin, player, scout_path, scout_card_index):
        self.occupied = True
        if self.segment_type == "caesar":
            player["has_greeted_caesar"] = True
        self.producer.send(request_origin, value={
            "event": "scout_result",
            "request_origin": request_origin,
            "result": "success",
            "player": player,
            "scout_path": scout_path,
        })
        cards = player.get("cards")
        for (i, segment) in enumerate(scout_path):
            if i == 0: # start segment
                continue
            if segment.endswith("-0"):
                player["round"] = player.get("round") + 1
                if player.get("round") == 3:
                    if player.get("has_greeted_caesar"):
                        print(f"Player {player.get('player_id')} has finished!")
                    else:
                        print(f"Player {player.get('player_id')} has finished, but has not greeted Caesar. He lost.")
                    self.occupied = False
                    return
                else:
                    print(f"Player {player.get('player_id')} finished round {player.get('round')}.")
        # Exchange used card with new card
        old_card = cards.pop(scout_card_index)
        new_card = draw_card()
        cards.append(new_card)
        cards = sorted(cards, reverse = True)
        player["cards"] = cards
        print(f"Player {player.get('player_id')} used card {old_card} and drew card {new_card}. New cards: {cards}")
        sleep_time = 5
        print(f"Player {player.get('player_id')} moved from {request_origin} to {self.segment_id} using this path: {scout_path}. Waiting {sleep_time} seconds...")
        sleep(sleep_time)
        if len(cards) == 0:
            print(f"Player {player.get('player_id')} has no more cards. He lost.")
            return
        else:
            # Play next card
            self.start_scout(player, self.segment_id, [self.segment_id], cards[0], 0)


    def handle_scout(self, scout_steps, request_origin, player, scout_path, scout_card_index):
        # print(f"Player {player.get('player_id')} is scouting {self.segment_id}")
        if self.occupied and request_origin != self.segment_id:
            scout_path.pop() # get rid of this segment
            last_visited_segment = scout_path.pop() # find last segment
            # print(f"Failed to scout {self.segment_id} because it is occupied. Trying next segment of {last_visited_segment}.")
            self.producer.send(last_visited_segment, value={
                "event": "scout_result",
                "request_origin": request_origin,
                "result": "failure",
                "player": player,
            })
            return
        if scout_steps == 1:
            self.move_player_to_this_segment(request_origin, player, scout_path, scout_card_index)
        else:
            self.start_scout(player, request_origin, scout_path, scout_steps-1, scout_card_index)


def handle_message(msg, segment, segment_id):
    """Processes a message in a separate thread."""
    if msg.value.get("event") == "start":
        player = msg.value.get("player")
        segment.start_scout(player, segment_id, [segment_id], player.get("cards")[0], 0)

    elif msg.value.get("event") == "scout_result":
        if msg.value.get("result") == "success":
            segment.occupied = False
        elif segment.scouting_states is not None:
            segment.handle_scout_failure(msg.value.get("request_origin"), msg.value.get("player"))

    elif msg.value.get("event") == "scout":
        segment.handle_scout(
            msg.value.get("scout_steps"),
            msg.value.get("request_origin"),
            msg.value.get("player"),
            msg.value.get("scout_path"),
            msg.value.get("scout_card_index"))


def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} segment_id segment_type next_segment1,next_segment2,...")
        sys.exit(1)
    segment_id = sys.argv[1]
    segment_type = sys.argv[2]
    next_segments = sys.argv[3].split(",")
    segment = Segment(segment_id, segment_type, next_segments)
    try:
        for msg in segment.consumer:
            thread = threading.Thread(target=handle_message, args=(msg, segment, segment_id))
            thread.start()
    except KeyboardInterrupt:
        segment.consumer.close()
        sys.exit(0)

if __name__ == "__main__":
    main()
