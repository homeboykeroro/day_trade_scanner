import asyncio
import threading
import aiohttp
import time

from utils.logger import Logger

logger = Logger()
loop = asyncio.new_event_loop()

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def fetch(session: aiohttp.ClientSession(), method: str, endpoint: str, payload: dict, semaphore, headers: dict = None):
    async with semaphore:
        json_response = None
        
        try:
            if method == 'GET':
                logger.log_debug_msg(f"GET request with payload: {payload} send")
                async with session.get(endpoint, params=payload, ssl=False, headers=headers) as response:
                    json_response = await response.json()
                    logger.log_debug_msg(f"GET request with payload: {payload} response: {json_response}")
                    return json_response
            elif method == 'POST':
                async with session.post(endpoint, json=payload, ssl=False, headers=headers) as response:
                    json_response = await response.json()
                    logger.log_debug_msg(f"POST request with payload: {payload} response: {json_response}")
                    return await response.json()
            elif method == 'DELETE':
                async with session.delete(endpoint, json=payload, ssl=False, headers=headers) as response:
                    json_response = await response.json()
                    logger.log_debug_msg(f"POST request with payload: {payload} response: {json_response}")
                    return await response.json()
        except Exception as e:
            status_code = 500 if not hasattr(json_response, 'status') else json_response.status
            logger.log_error_msg(f'Error during {method} request to {endpoint}, payload: {payload}, Cause: {e}, Status code: {status_code}')
            return {'status': 'FAILED', 'statusCode:': {status_code}, 'errorMsg': str(e), 'payload': payload}

async def process_async_request(method: str, endpoint: str, payload_list: list, chunk_size: int, no_of_request_per_sec: int, headers: dict = None) -> dict:
    semaphore = asyncio.Semaphore(chunk_size)  # Limit to chunk_size concurrent requests
    result_dict = {'response_list': [], 'error_response_list': []}
    
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(payload_list), chunk_size):
            chunk = payload_list[i:i + chunk_size]
            tasks = [asyncio.create_task(fetch(session, method, endpoint, payload, semaphore, headers)) for payload in chunk]
            
            all_chunk_start_time = time.time()
            response_list = await asyncio.gather(*tasks, return_exceptions=True)
            logger.log_debug_msg(f'Completion of chunk time: {time.time() - all_chunk_start_time} seconds')
            
            for response in response_list:
                isError = isinstance(response, Exception)
                if isError or 'errorMsg' in response:
                    result_dict['error_response_list'].append(response)
                else:
                    result_dict['response_list'].append(response)
            
            # Wait after processing each chunk
            if no_of_request_per_sec:
                logger.log_debug_msg(f'Waiting {no_of_request_per_sec} seconds before processing next chunk')
                await asyncio.sleep(no_of_request_per_sec)
    
    return result_dict
        
def send_async_request(method: str, endpoint: str, payload_list: list, chunk_size: int, no_of_request_per_sec: float = None, headers: dict = None, loop = None):
    if loop is None:
        logger.log_debug_msg(f'No event loop is set for {endpoint}, caller thread: {threading.current_thread().name}')
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    else:
        logger.log_debug_msg(f'Use event loop passed from {threading.current_thread().name}')
    
    response_result = loop.run_until_complete(process_async_request(method, endpoint, payload_list, chunk_size, no_of_request_per_sec, headers))
    loop.close()
    
    response_list = response_result['response_list']
    error_response_list = response_result['error_response_list']
    
    if len(error_response_list) > 0:
        for error_response in error_response_list:
            status_code = error_response.get('statusCode')
            if status_code == 401:
                raise aiohttp.ClientError(f'Client Portal Connection Error, response: {error_response}')
            else:
                raise Exception(f'HTTP Request Fatal Error, response: {error_response}')
    
    return response_list