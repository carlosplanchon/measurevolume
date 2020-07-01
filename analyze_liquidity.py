#!/usr/bin/env python3

from json import loads

from vtclear import clear_screen

import matplotlib.pyplot as plt

from typing import List

# from time import sleep

plt.style.use("dark_background")


def get_bids_volume(actual_bids, old_bids):
    # print("#" * 50)
    # print(f"Actual bids length: {len(actual_bids)}")
    # print(f"Old bids length: {len(old_bids)}")
    # print("#" * 50)

    volume = 0

    unmatched_volume = 0

    old_bids_offset = 0

    i = 0
    stop = False
    while stop is False:
        # print(f"i: {i}")
        # print(f"old_bids_offset: {old_bids_offset}")

        if i + old_bids_offset < len(old_bids) and i < len(actual_bids):
            actual_bid = actual_bids[i]
            old_bid = old_bids[i + old_bids_offset]
            # print(f"Actual bid: {actual_bid}")
            # print(f"Old bid: {old_bid}")

            if float(actual_bid[0]) > float(old_bid[0]):
                # Highest bids are on the actual bid side.
                # print("· Highest bids are on the actual bid side.")
                i += 1
                old_bids_offset -= 1

            if float(actual_bid[0]) < float(old_bid[0]):
                # Lowest bids are on the actual bid side.
                # print("· Lowest bids are on the actual bid side.")
                i += 0
                old_bids_offset += 1

                # price * qty.
                qty_difference = float(old_bid[1]) * float(old_bid[0])
                # print(f"\t- Qty difference: {qty_difference}")

                unmatched_volume += qty_difference

            if float(actual_bid[0]) == float(old_bid[0]):
                # print("· Actual bid price and old bid price match.")
                if unmatched_volume > 0:
                    volume += unmatched_volume
                unmatched_volume = 0

                # price * old_qty - actual_qty.
                plevel_volume = (
                    float(old_bid[1]) - float(
                        actual_bid[1])) * float(actual_bid[0])
                # print(
                #     f"volume at this price level: {plevel_volume}")
                if plevel_volume > 0:
                    volume += plevel_volume

                i += 1
                old_bids_offset += 0
        else:
            stop = True
        # input("CONTINUE")

    return volume


def get_asks_volume(actual_asks, old_asks):
    # clear_screen()
    # print("OLD ASKS")
    # pprint(old_asks)
    # print("ACTUAL ASKS")
    # pprint(actual_asks)
    # print("#" * 50)
    # print(f"Actual asks length: {len(actual_asks)}")
    # print(f"Old asks length: {len(old_asks)}")
    # print("#" * 50)

    volume = 0

    unmatched_volume = 0

    old_asks_offset = 0

    i = 0
    stop = False
    while stop is False:
        # print(f"i: {i}")
        # print(f"old_asks_offset: {old_asks_offset}")

        if i + old_asks_offset < len(old_asks) and i < len(actual_asks):
            actual_ask = actual_asks[i]
            old_ask = old_asks[i + old_asks_offset]
            # print(f"Actual ask: {actual_ask}")
            # print(f"Old ask: {old_ask}")

            if float(actual_ask[0]) < float(old_ask[0]):
                # Lowest asks are on the actual ask side.
                # print("· Lowest asks are on the actual ask side.")
                i += 1
                old_asks_offset -= 1

            if float(actual_ask[0]) > float(old_ask[0]):
                # Higher asks are on the actual ask side.
                # print("· Higher asks are on the actual ask side.")
                i += 0
                old_asks_offset += 1

                # price * qty.
                qty_difference = float(old_ask[1]) * float(old_ask[0])
                # print(f"\t- Qty difference: {qty_difference}")

                unmatched_volume += qty_difference

            if float(actual_ask[0]) == float(old_ask[0]):
                # print("· Actual ask price and old ask price match.")
                if unmatched_volume > 0:
                    volume += unmatched_volume
                unmatched_volume = 0

                # price * old_qty - actual_qty.
                plevel_volume = (
                    float(old_ask[1]) - float(
                        actual_ask[1])) * float(actual_ask[0])
                # print(
                #     f"volume at this price level: {plevel_volume}")
                if plevel_volume > 0:
                    volume += plevel_volume

                i += 1
                old_asks_offset += 0
        else:
            stop = True
        # input("CONTINUE")

    # print(f"> ASKS VOLUME: {volume}")
    # sleep(0.1)
    return volume


def analyze_liquidity(EX1, EX2, ob_log_list):
    ex1_bids = None
    ex1_asks = None

    ex2_bids = None
    ex2_asks = None

    ex1_bids_volume_timestamp_list = []
    ex2_bids_volume_timestamp_list = []
    ex1_bids_volume_list: List[float] = []
    ex2_bids_volume_list: List[float] = []

    ex1_asks_volume_timestamp_list = []
    ex2_asks_volume_timestamp_list = []
    ex1_asks_volume_list: List[float] = []
    ex2_asks_volume_list: List[float] = []

    for line in range(len(ob_log_list)):
        clear_screen()
        print(f"Analyzing: {(line / len(ob_log_list)) * 100:.2f}%")
        data = loads(ob_log_list[line])

        ##################
        #   EXCHANGE 1   #
        ##################
        if data["exchange"] == EX1:
            old_ex1_bids = ex1_bids
            old_ex1_asks = ex1_asks

            ex1_bids = data["bids"]
            ex1_asks = data["asks"]

            # Bids change analysis.
            # print("· BIDS CHANGE ANALYSIS.")
            if old_ex1_bids is not None and ex1_bids is not None:
                ex1_bids_volume = get_bids_volume(
                    actual_bids=ex1_bids,
                    old_bids=old_ex1_bids
                )
                # print(f"- BIDS VOLUME: {ex1_bids_volume}")
                ex1_bids_volume_list.append(ex1_bids_volume)
                ex1_bids_volume_timestamp_list.append(data["timestamp"])

            # Asks change analysis.
            # print("· ASKS CHANGE ANALYSIS.")
            if old_ex1_asks is not None and ex1_asks is not None:
                ex1_asks_volume = get_asks_volume(
                    actual_asks=ex1_asks,
                    old_asks=old_ex1_asks
                )
                # print(f"- ASKS VOLUME: {ex1_asks_volume}")
                ex1_asks_volume_list.append(ex1_asks_volume)
                ex1_asks_volume_timestamp_list.append(data["timestamp"])

        ##################
        #   EXCHANGE 2   #
        ##################
        if data["exchange"] == EX2:
            old_ex2_bids = ex2_bids
            old_ex2_asks = ex2_asks

            ex2_bids = data["bids"]
            ex2_asks = data["asks"]

            # Bids change analysis.
            # print("· BIDS CHANGE ANALYSIS")
            if old_ex2_bids is not None and ex2_bids is not None:
                ex2_bids_consumption = get_bids_volume(
                    actual_bids=ex2_bids,
                    old_bids=old_ex2_bids
                )
                # print(f"- BIDS CONSUMPTION: {ex2_bids_consumption}")
                ex2_bids_volume_list.append(ex2_bids_consumption)
                ex2_bids_volume_timestamp_list.append(data["timestamp"])

            # Asks change analysis.
            # print("· ASKS CHANGE ANALYSIS.")
            if old_ex2_asks is not None and ex2_asks is not None:
                ex2_asks_volume = get_asks_volume(
                    actual_asks=ex2_asks,
                    old_asks=old_ex2_asks
                )
                # print(f"- ASKS VOLUME: {ex2_asks_volume}")
                ex2_asks_volume_list.append(ex2_asks_volume)
                ex2_asks_volume_timestamp_list.append(data["timestamp"])

    return ex1_bids_volume_timestamp_list, ex1_bids_volume_list,\
        ex2_bids_volume_timestamp_list, ex2_bids_volume_list,\
        ex1_asks_volume_timestamp_list, ex1_asks_volume_list,\
        ex2_asks_volume_timestamp_list, ex2_asks_volume_list


if __name__ == "__main__":
    EX1 = "EX1"
    EX2 = "EX2"

    ob_log_filename = "ORDER_BOOK.csv"

    clear_screen()
    print("· LOADING FILES...")
    with open(ob_log_filename, "r") as f:
        order_book_content = f.read()

    ob_log_list = order_book_content.split("\n")[:-1]

    ex1_bids_volume_timestamp_list, ex1_bids_volume_list,\
        ex2_bids_volume_timestamp_list, ex2_bids_volume_list,\
        ex1_asks_volume_timestamp_list, ex1_asks_volume_list,\
        ex2_asks_volume_timestamp_list, ex2_asks_volume_list\
        = analyze_liquidity(
            EX1=EX1,
            EX2=EX2,
            ob_log_list=ob_log_list
        )

    with open("analysis.txt", "w") as f:
        f.write(str(ex1_bids_volume_timestamp_list) + "\n")
        f.write(str(ex1_bids_volume_list) + "\n")
        f.write(str(ex2_bids_volume_timestamp_list) + "\n")
        f.write(str(ex2_bids_volume_list) + "\n")
        f.write(str(ex1_asks_volume_timestamp_list) + "\n")
        f.write(str(ex1_asks_volume_list) + "\n")
        f.write(str(ex2_asks_volume_timestamp_list) + "\n")
        f.write(str(ex2_asks_volume_list))

    clear_screen()
    print("--- RESULTS ---")
    ex1_bids_volume = sum(ex1_bids_volume_list)
    ex2_bids_volume = sum(ex2_bids_volume_list)
    ex1_asks_volume = sum(ex1_asks_volume_list)
    ex2_asks_volume = sum(ex2_asks_volume_list)
    print("-" * 30)
    print(f"EX1 bids volume: {ex1_bids_volume:.2f}")
    print(f"EX2 bids volume: {ex2_bids_volume:.2f}")
    print(f"EX1 asks volume: {ex1_asks_volume:.2f}")
    print(f"EX2 asks volume: {ex2_asks_volume:.2f}")
    print("-" * 30)

    ex1_lapse = max(
        ex1_asks_volume_timestamp_list[-1],
        ex1_bids_volume_timestamp_list[-1]
    ) - min(
            ex1_asks_volume_timestamp_list[0],
            ex1_bids_volume_timestamp_list[0]
    )

    ex1_avg_hourly_volume = (
        (ex1_bids_volume + ex1_asks_volume) / ex1_lapse) * 3600

    ex2_lapse = max(
        ex2_asks_volume_timestamp_list[-1],
        ex2_bids_volume_timestamp_list[-1]
    ) - min(
        ex2_asks_volume_timestamp_list[0],
        ex2_bids_volume_timestamp_list[0]
    )

    ex2_avg_hourly_volume = (
        (ex2_bids_volume + ex2_asks_volume) / ex2_lapse) * 3600

    print(f"EX1 average hourly volume: {ex1_avg_hourly_volume:.2f}")
    print(f"EX2 average hourly volume: {ex2_avg_hourly_volume:.2f}")
    print("-" * 30)

    plt.grid(c="#222222")
    plt.plot(
        ex1_bids_volume_timestamp_list, ex1_bids_volume_list,
        label="EX1 BIDS", lw=0.5, c="#00ff00"
    )
    plt.plot(
        ex2_bids_volume_timestamp_list, ex2_bids_volume_list,
        label="EX2 BIDS", lw=0.5, c="#00ffff"
    )

    plt.plot(
        ex1_asks_volume_timestamp_list, ex1_asks_volume_list,
        label="EX1 ASKS", lw=0.5, c="#ff0000"
    )
    plt.plot(
        ex2_asks_volume_timestamp_list, ex2_asks_volume_list,
        label="EX2 ASKS", lw=0.5, c="#ffff00"
    )
    plt.legend()
    plt.show()
