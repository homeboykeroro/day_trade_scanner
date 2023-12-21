import asyncio
import aiohttp
import time
import json

from utils.logger import Logger

logger = Logger()

async def fetch(session: aiohttp.ClientSession(), method: str, endpoint: str, payload: dict, semaphore):
    async with semaphore:
        try:
            if method == 'GET':
                async with session.get(endpoint, params=payload, ssl=False) as response:
                    return await response.json()
            else:
                async with session.post(endpoint, json=payload, ssl=False) as response:
                    return await response.json()
        except Exception as e:
            logger.log_error_msg(f'Error during {method} request to {endpoint}, payload: {payload}, Cause: {e}')
            return {'status': 'FAILED', 'errorMsg': str(e), 'payload': payload}

async def process_async_request(method: str, endpoint: str, payload_list: list, chunk_size: int, request_wait_time: float) -> dict:
    semaphore = asyncio.Semaphore(chunk_size)  # Limit to 5 concurrent requests
    result_dict = {'response_list': [], 'error_response_list': []}
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        
        all_chunk_start_time = time.time()
        
        for payload in payload_list:
            task = asyncio.create_task(fetch(session, method, endpoint, payload, semaphore))
            tasks.append(task)
        
            if request_wait_time:
                logger.log_debug_msg(f'Wait {request_wait_time} seconds')
                asyncio.sleep(request_wait_time)
        
        response_list = await asyncio.gather(*tasks, return_exceptions=True)
        logger.log_debug_msg(f'Completion of all async requests time: {time.time() - all_chunk_start_time} seconds')
        
        for response in response_list:
            if 'errorMsg' in response:
                result_dict['error_response_list'].append(response)
            else:
                result_dict['response_list'].append(response)
        
    return result_dict
        
def send_async_request(method: str, endpoint: str, payload_list: list, chunk_size: int, request_wait_time: float = None):
    response_result = asyncio.run(process_async_request(method, endpoint, payload_list, chunk_size, request_wait_time))
    response_list = response_result['response_list']
    error_response_list = response_result['error_response_list']
    
    if len(error_response_list) > 0:
        raise Exception(f'Async requests not completed successfully, error response list: {json.dumps(error_response_list)}')
    
    return response_list