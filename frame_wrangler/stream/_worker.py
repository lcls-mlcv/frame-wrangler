def _evaluate_chunk(raw: bytes, func) -> bool:
    """
    Module-level worker function for multiprocessing.Pool.
    Must be defined at module level to be picklable.
    """
    from frame_wrangler.stream.chunk import Chunk
    return bool(func(Chunk(raw)))
