import sys, json
import time
from kafka import KafkaProducer
import subprocess

from utils import draw_card

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092', 'localhost:39092', 'localhost:49092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def init_player(segment_id):
    cards = []
    for _ in range(3):
        cards.append(draw_card())
        cards = sorted(cards, reverse = True)
    return {
        "player_id": segment_id.split("-")[-1],
        "round": 0,
        "has_greeted_caesar": False,
        "cards": cards,
    }

def read_tracks(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f).get("tracks")


def start_segment(type: str, segment_id: str, next_segments: list[str]):
    print(f"Starting segment {segment_id}")
    process = subprocess.Popen(
        "python segment.py " + segment_id + " " + ",".join(next_segments),
        shell=True,
        text=True  # Ensures output is treated as text instead of bytes
    )
    return process  # Process continues in background!


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} /path/to/track_file.json")
        sys.exit(1)
    tracks = read_tracks(sys.argv[1])

    segments = []
    for track in tracks:
        for segment in track.get("segments"):
            segment_id = segment.get("segmentId")
            segments.append((
                segment,
                start_segment(
                    segment.get("type"),
                    segment_id,
                    segment.get("nextSegments"))
            ))

    time.sleep(1)  # Wait for all segments to start

    print("Sending start events")
    for segment, process in segments:
        if segment.get("type") == "start-goal":
            producer.send(segment.get("segmentId"),
                {
                    "event": "start",
                    "player": init_player(segment.get("segmentId"))
                }
            )

    for segment, process in segments:
        process.wait()


if __name__ == "__main__":
    main()
