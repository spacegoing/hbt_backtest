from typing import Optional

import numpy as np
from numpy.typing import NDArray

from ... import HftBacktest, Linear, ConstantLatency
from ...typing import DataCollection, Data
from ...reader import UNTIL_END_OF_DATA


def create_last_snapshot(
        data: DataCollection,
        tick_size: float,
        lot_size: float,
        initial_snapshot: Optional[Data] = None,
        output_snapshot_filename: Optional[str] = None
) -> NDArray:
    r"""
    Creates a snapshot of the last market depth for the specified data, which can be used as the initial snapshot data
    for subsequent data.

    Args:
         data: Data to be processed to obtain the last market depth snapshot.
         tick_size: Minimum price increment for the given asset.
         lot_size: Minimum order quantity for the given asset.
         initial_snapshot: The initial market depth snapshot.
         output_snapshot_filename: If provided, the snapshot data will be saved to the specified filename in ``npz``
                                   format.

    Returns:
        Snapshot of the last market depth compatible with HftBacktest.
    """
    # Just to reconstruct order book from the given snapshot to the end of the given data.
    hbt = HftBacktest(
        data,
        tick_size,
        lot_size,
        0,
        0,
        ConstantLatency(0, 0),
        Linear,
        snapshot=initial_snapshot
    )

    # Go to the end of the data.
    hbt.goto(UNTIL_END_OF_DATA)

    snapshot = []
    snapshot += [[
        4,
        hbt.last_timestamp,
        -1,
        1,
        float(bid * tick_size),
        float(qty)
    ] for bid, qty in sorted(hbt.bid_depth.items(), key=lambda v: -float(v[0]))]
    snapshot += [[
        4,
        hbt.last_timestamp,
        -1,
        -1,
        float(ask * tick_size),
        float(qty)
    ] for ask, qty in sorted(hbt.ask_depth.items(), key=lambda v: float(v[0]))]

    snapshot = np.asarray(snapshot, np.float64)

    if output_snapshot_filename is not None:
        np.savez(output_snapshot_filename, data=snapshot)

    return snapshot
