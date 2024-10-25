
def split_long_paragraph_into_chunks(stacktrace: str, chunk_size: int):
    splited_paragraph_list = stacktrace.split('\n')
    chunk_list = []

    counter = 0
    total_no_of_char = 0
    concat_chunk_str = ''
    while counter < len(splited_paragraph_list):
        line_txt = splited_paragraph_list[counter]
        line_txt_length = len(line_txt)
        total_no_of_char += line_txt_length

        if total_no_of_char <= chunk_size:
            concat_chunk_str += line_txt
            counter += 1

            if counter == len(splited_paragraph_list) - 1:
                chunk_list.append(concat_chunk_str)
        else:
            chunk_list.append(concat_chunk_str)
            total_no_of_char = 0
            concat_chunk_str = ''