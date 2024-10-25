def get_chunk_list(input_list: list, chunk_size: int) -> list:
    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]