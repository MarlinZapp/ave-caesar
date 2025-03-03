import sys, json
import threading
from time import sleep
from kafka import KafkaProducer, KafkaConsumer
from utils import draw_card
from dataclasses import dataclass


@dataclass(frozen=True)
class Target:
    segment_id: str
    segment_type: str
    # concatenated path to reach this target
    path: str

    def to_dict(self):
        return {
            "segment_id": self.segment_id,
            "segment_type": self.segment_type,
            "path": self.path
        }

def target_from_dict(d: dict):
    return Target(d["segment_id"], d["segment_type"], d["path"])

@dataclass
class ScoutingState:
    scout_segment_index: int
    scout_path: list[str]
    scout_steps: int
    scout_for_card: int
    request_origin: str
    player: dict
    possible_targets: set[Target]
    visited_segments: list[str]
    targets_found_per_card: dict[str, set[Target]]


def remove_card(player: dict, card: int) -> dict:
        cards : list[int] = player["cards"]
        for i in range(len(cards)):
            if cards[i] == card:
                cards.pop(i)
                break
        player["cards"] = cards
        return player


class Segment:
    def __init__(self, segment_id : str, segment_type : str, next_segments : list[str]):
        self.segment_id = segment_id
        self.segment_type = segment_type
        self.next_segments = next_segments
        self.scouting_states : dict[str, ScoutingState] = dict()
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


    def scout(self, state: ScoutingState):
        state.scout_path.append(self.next_segments[state.scout_segment_index])
        if self.next_segments[state.scout_segment_index] in state.visited_segments:
            self.scout_next_segment(state)
            return

        serializable_targets = [target.to_dict() for target in state.possible_targets]

        self.producer.send(self.next_segments[state.scout_segment_index], value={
            "event": "scout",
            "request_origin": state.request_origin,
            "player": state.player,
            "scout_steps": state.scout_steps,
            "scout_path": state.scout_path,
            "scout_for_card": state.scout_for_card,
            "possible_targets": serializable_targets,
            "visited_segments": state.visited_segments
        })


    def start_scout(
        self, player, request_origin: str, scout_path: list[str], scout_steps: int, scout_for_card: int,
        possible_targets: set[Target], visited_segments: list[str]):
        # print(f"Starting scout from {request_origin} to search a path of length {player.get('cards')[0]}")
        # if request_origin == self.segment_id:
            # print(f"Player {player.get('player_id')} is scouting targets for card {player['cards'][0]}")
        state = ScoutingState(
            player=player,
            request_origin=request_origin,
            scout_segment_index=0,
            scout_path=scout_path,
            scout_steps=scout_steps,
            scout_for_card=scout_for_card,
            possible_targets=possible_targets,
            targets_found_per_card=dict(),
            visited_segments=visited_segments
        )
        self.scout(state)
        self.scouting_states[request_origin] = state


    def handle_scout_result(self, request_origin: str, scout_path: list[str], possible_targets: set[Target], visited_segments: list[str]):
        state = self.scouting_states[request_origin]
        for segment in visited_segments:
            if segment not in state.visited_segments:
                state.visited_segments.append(segment)
        for target in possible_targets:
            state.possible_targets.add(target)

        finished_scouting = self.scout_next_segment(state)
        if finished_scouting and request_origin == self.segment_id:
            self.move_player_to_best_segment(state)


    def scout_next_segment(self, scout_state: ScoutingState) -> bool:
        """
        Scout the next segment or card in the player's hand.
        Returns True if all possibilites have been tried.
        """
        player = scout_state.player
        request_origin = scout_state.request_origin
        cards : list[int] = player["cards"]
        segment_index = scout_state.scout_segment_index
        scout_state.scout_path.pop() # The segment that has been looked up in the last scout
        scout_state.scout_path.pop() # This segment
        previous_segment = None
        if len(scout_state.scout_path) > 0:
            previous_segment = scout_state.scout_path.pop() # Segment that lead to this segment

        # try next segment
        if segment_index is not None and segment_index < len(self.next_segments) - 1:
            segment_index = segment_index + 1
            scout_state.scout_segment_index = segment_index
            if previous_segment is not None:
                scout_state.scout_path.append(previous_segment)
            scout_state.scout_path.append(self.segment_id)
            self.scout(scout_state)
        # try next card if scouting started from this segment and all segments have been scouted
        else:
            next_card = None
            if request_origin == self.segment_id:
                old_card = scout_state.scout_for_card
                scout_state.targets_found_per_card[str(old_card)] = scout_state.possible_targets
                for card in cards:
                    if str(card) not in scout_state.targets_found_per_card.keys():
                        next_card = card

            if next_card is not None:
                scout_state.scout_segment_index = 0
                scout_state.scout_for_card = next_card
                scout_state.scout_steps = next_card
                scout_state.scout_path = [self.segment_id]
                scout_state.possible_targets = set()
                scout_state.visited_segments = []
                # print(f"Player {player.get('player_id')} is scouting targets for card {next_card}")
                self.scout(scout_state)

            # Continue scouting from previous segment or finish scouting
            else:
                if previous_segment is not None:
                    # print(f"Tried all ways from {self.segment_id}, moving back to {previous_segment}.")
                    serializable_targets = [target.to_dict() for target in scout_state.possible_targets]
                    self.producer.send(previous_segment, value={
                        "event": "scout_result",
                        "request_origin": request_origin,
                        "player": player,
                        "possible_targets": serializable_targets,
                        "visited_segments": scout_state.visited_segments + [self.segment_id]
                    })
                else:
                    return True
        return False


    def move_player_to_best_segment(self, scout_state: ScoutingState):
        player = scout_state.player
        possible_targets = scout_state.possible_targets
        if len(possible_targets) == 0:
            print(f"Player {player.get('player_id')} can not move. Skipping turn and waiting 10 seconds...")
            sleep(10)
            card = player["cards"][0]
            self.start_scout(scout_state.player, self.segment_id, [self.segment_id], card, card, set(), [])
            return
        preferred_card = None
        preferred_target = None
        if not player["has_greeted_caesar"]:
            for (card, targets) in scout_state.targets_found_per_card.items():
                for target in targets:
                    if target.segment_type == "caesar":
                        preferred_card = int(card)
                        preferred_target = target
                        break

        if preferred_card is None or preferred_target is None:
            highest_card = 0
            for (card, targets) in scout_state.targets_found_per_card.items():
                if int(card) > highest_card:
                    highest_card = int(card)
                    preferred_card = highest_card
                    preferred_target = list(targets)[0]

        if preferred_card is None or preferred_target is None:
            raise Exception("No preferred target found.")
        self.occupied = False
        player = remove_card(player, preferred_card)
        self.producer.send(preferred_target.segment_id, value={
            "event": "move",
            "player": player,
            "path": preferred_target.path,
        })
        return


    def handle_move(self, player: dict, joined_path: str):
        path = joined_path.split(",")
        print(f"Player {player.get('player_id')} moved from {path[0]} to {self.segment_id} using this path: {path}.")
        if self.segment_type == "caesar":
            player["has_greeted_caesar"] = True
            print(f"Player {player.get('player_id')} greeted Caesar.")
        for (i, segment) in enumerate(path):
            if i == 0: # start segment
                continue
            if segment.endswith("-0"):
                player["round"] = player["round"] + 1
                if player.get("round") == 3:
                    if player.get("has_greeted_caesar"):
                        print(f"Player {player.get('player_id')} has finished!")
                    else:
                        print(f"Player {player.get('player_id')} has finished, but has not greeted Caesar. He lost.")
                    self.occupied = False
                    return
                else:
                    print(f"Player {player.get('player_id')} finished round {player.get('round')}.")
        new_card = draw_card()
        player["cards"].append(new_card)
        sleep_time = 10
        print(f"Player {player.get('player_id')} used card {len(path)-1} and drew card {new_card}. New cards: {player['cards']}. Waiting {sleep_time} seconds...")
        sleep(sleep_time)
        if len(player["cards"]) == 0:
            print(f"Player {player.get('player_id')} has no more cards. He lost.")
            return
        else:
            # Play next card
            card = player["cards"][0]
            self.start_scout(player, self.segment_id, [self.segment_id], card, card, set(), [])


    def found_target(self, request_origin: str, scout_path: list[str], scout_for_card: int):
        target = Target(segment_id=self.segment_id, segment_type=self.segment_type, path=",".join(scout_path)).to_dict()
        target_answer : list[dict] = [target]
        # Send success message to previous segment
        self.producer.send(scout_path[-2], value={
            "event": "scout_result",
            "request_origin": request_origin,
            "scout_path": scout_path,
            "possible_targets": target_answer,
            "visited_segments": [self.segment_id]
        })


    def handle_scout(
        self, scout_steps: int, request_origin: str, player, scout_path: list[str], scout_for_card: int,
        possible_targets: set[Target], visited_segments: list[str]):
        # print(f"Player {player.get('player_id')} is scouting {self.segment_id}")
        if self.occupied and request_origin != self.segment_id:
            scout_path.pop() # get rid of this segment
            last_visited_segment = scout_path.pop() # find last segment
            # print(f"Failed to scout {self.segment_id} because it is occupied. Trying next segment of {last_visited_segment}.")
            self.producer.send(last_visited_segment, value={
                "event": "scout_result",
                "request_origin": request_origin,
                "possible_targets": dict(), # no new targets
                "visited_segments": [self.segment_id]
            })
            return
        if scout_steps == 1:
            self.found_target(request_origin, scout_path, scout_for_card)
        else:
            self.start_scout(player, request_origin, scout_path, scout_steps-1, scout_for_card, possible_targets, visited_segments)


    def handle_message(self, msg):
        """Processes a message in a separate thread."""

        possible_targets_json : list[dict] | None = msg.get("possible_targets")
        possible_targets = set([target_from_dict(target) for target in possible_targets_json or []])

        if msg.get("event") == "start":
            player = msg.get("player")
            card = player.get("cards")[0]
            self.start_scout(player, self.segment_id, [self.segment_id], card, card, set(), [])

        elif msg.get("event") == "scout_result":
            self.handle_scout_result(
                msg.get("request_origin"),
                msg.get("scout_path"),
                possible_targets,
                msg.get("visited_segments")
            )

        elif msg.get("event") == "scout":
            self.handle_scout(
                msg.get("scout_steps"),
                msg.get("request_origin"),
                msg.get("player"),
                msg.get("scout_path"),
                msg.get("scout_for_card"),
                possible_targets,
                msg.get("visited_segments")
            )

        elif msg.get("event") == "move":
            self.handle_move(msg.get("player"), msg.get("path"))


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
            thread = threading.Thread(target=segment.handle_message, args=(msg.value,))
            thread.start()
    except KeyboardInterrupt:
        segment.consumer.close()
        sys.exit(0)

if __name__ == "__main__":
    main()
