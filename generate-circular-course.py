#!/usr/bin/env python3
from random import randint
import sys
import json

def get_next_segments(segment_types: list[str], track: int, num_tracks, segment: int, num_segments: int):
    seg_type = segment_types[segment]
    next_segment = (segment + 1) % num_segments
    next_seg_type = segment_types[next_segment]
    segments = []
    # Additional caesar greeting segment track
    if next_seg_type == "start-goal":
        num_tracks = num_tracks + 1
    for next_track in range(1, num_tracks + 1):
        if next_seg_type == "bottleneck":
            if next_track != 1:
                continue
            # In bottleneck segments, only the first track exists
            segments.append(f"segment-{next_track}-{next_segment}")
        elif next_seg_type == "wall-divided":
            if seg_type == "wall-divided":
                # You must stay on this track in two consecutive wall-divided segments
                if track == next_track:
                    segments.append(f"segment-{track}-{next_segment}")
            elif abs(track - next_track) <= 1 or seg_type == "bottleneck":
                # You can only switch to adjacent tracks in a wall-divided segment except for when the current segment is a bottleneck
                segments.append(f"segment-{next_track}-{next_segment}")
        else:
            if seg_type == "wall-divided":
                if abs(track - next_track) <= 1:
                    # After a wall-divided segment, you can only switch to adjacent tracks
                    segments.append(f"segment-{next_track}-{next_segment}")
            else:
                # By default, players can switch to any track
                segments.append(f"segment-{next_track}-{next_segment}")
    return segments


def generate_tracks(num_tracks: int, num_segments: int):
    """
    Generates a data structure with 'num_tracks' circular tracks.
    Each track has exactly 'length_of_track' segments:
      - 1 segment: 'segment-t-0'
      - (length_of_track - 1) segments: 'segment-t-c'
    Returns a Python dict that can be serialized to JSON.
    """
    all_tracks = []

    segment_types = ["start-goal"]
    for track in range(2, num_segments + 1):
        r = randint(1, 100)
        if r <= 15:
            segment_types.append("bottleneck")
        elif r <= 55:
            segment_types.append("wall-divided")
        else:
            segment_types.append("normal")

    # Add additional track for Caesar greeting segment
    all_tracks.append({
        "trackId" : str(num_tracks + 1),
        "segments" : [{
            "segmentId" : f"segment-{num_tracks + 1}-0",
            "type" : "caesar",
            "nextSegments" : get_next_segments(segment_types, num_tracks + 1, num_tracks, 0, num_segments)
        }]
    })

    for track in range(1, num_tracks + 1):
        track_id = str(track)
        segments = []

        # First segment: segment-t-0
        start_segment_id = f"segment-{track}-0"

        start_segment = {
            "segmentId": start_segment_id,
            "type": "start-goal",
            "nextSegments": get_next_segments(segment_types, track, num_tracks, 0, num_segments)
        }
        segments.append(start_segment)

        # Create normal segments: segment-t-c for c in [1..(L-1)]
        for segment in range(1, num_segments):
            if segment_types[segment] == "bottleneck" and track != 1:
                continue

            seg_id = f"segment-{track}-{segment}"

            segment = {
                "segmentId": seg_id,
                "type": segment_types[segment],
                "nextSegments": get_next_segments(segment_types, track, num_tracks, segment, num_segments)
            }
            segments.append(segment)

        track_definition = {
            "trackId": track_id,
            "segments": segments
        }
        all_tracks.append(track_definition)

    return {"tracks": all_tracks}


def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <num_tracks> <num_segments> <output_file>")
        sys.exit(1)

    num_tracks = int(sys.argv[1])
    num_segments = int(sys.argv[2])
    output_file = sys.argv[3]

    tracks_data = generate_tracks(num_tracks, num_segments)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(tracks_data, f, indent=2)
        f.write('\n')
    print(f"Successfully generated {num_tracks} track(s) of length {num_segments} into '{output_file}'")

if __name__ == "__main__":
    main()
