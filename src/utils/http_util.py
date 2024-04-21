import asyncio
import queue
import aiohttp
import time
import json

from utils.logger import Logger

from constant.endpoint.ib.client_portal_api_endpoint import ClientPortalApiEndpoint

logger = Logger()
loop = asyncio.new_event_loop()

ENDPOINT_TO_RATE_LIMIT_SEMAPHORE = {
    ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.SNAPSHOT: dict(
        CONCURRENT_CONTROL = asyncio.Semaphore(10),
        RATE_LIMIT_IN_SECOND = 1,
        QUEUE = queue.Queue()
    ),
    ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.MARKET_DATA_HISTORY: dict(
        CONCURRENT_CONTROL = asyncio.Semaphore(5),
        QUEUE = queue.Queue()
    ),
    ClientPortalApiEndpoint.HOSTNAME + ClientPortalApiEndpoint.RUN_SCANNER: dict(
        CONCURRENT_CONTROL = asyncio.Semaphore(1), 
        RATE_LIMIT_IN_SECOND = 1,
        QUEUE = queue.Queue()
    )
}

async def fetch(session: aiohttp.ClientSession(), method: str, endpoint: str, payload: dict, semaphore):
    rate_limit_semaphore = semaphore if semaphore else ENDPOINT_TO_RATE_LIMIT_SEMAPHORE.get(endpoint).get('CONCURRENT_CONTROL')
    
    async with rate_limit_semaphore:
        try:
            if method == 'GET':
                logger.log_debug_msg(f"GET request with payload: {payload} send")
                async with session.get(endpoint, params=payload, ssl=False) as response:
                    json_response = await response.json()
                    logger.log_debug_msg(f"GET request with payload: {payload} response: {json_response}")
                    return json_response
            elif method == 'POST':
                async with session.post(endpoint, json=payload, ssl=False) as response:
                    json_response = await response.json()
                    logger.log_debug_msg(f"POST request with payload: {payload} response: {json_response}")
                    return await response.json()
            elif method == 'DELETE':
                async with session.delete(endpoint, json=payload, ssl=False) as response:
                    json_response = await response.json()
                    logger.log_debug_msg(f"POST request with payload: {payload} response: {json_response}")
                    return await response.json()
        except Exception as e:
            logger.log_error_msg(f'Error during {method} request to {endpoint}, payload: {payload}, Cause: {e}, Status code: {response.status}')
            return {'status': 'FAILED', 'errorMsg': str(e), 'payload': payload}

async def process_async_request(method: str, endpoint: str, payload_list: list, chunk_size: int, no_of_request_per_sec: int) -> dict:
    try:
        semaphore = asyncio.Semaphore(chunk_size) if chunk_size else None # Limit to chunk_size concurrent requests
    except Exception as e:
        print()
        
    result_dict = {'response_list': [], 'error_response_list': []}
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        
        all_chunk_start_time = time.time()
        
        payload_list_queue = ENDPOINT_TO_RATE_LIMIT_SEMAPHORE.get(endpoint).get('QUEUE') if endpoint in ENDPOINT_TO_RATE_LIMIT_SEMAPHORE else None
        if payload_list_queue:
            i = 0
            semaphore = ENDPOINT_TO_RATE_LIMIT_SEMAPHORE.get(endpoint).get('CONCURRENT_CONTROL')
            while not payload_list_queue.empty():
                payload = payload_list_queue.get()
                logger.log_debug_msg(f'message queue payload: {payload}')
                task = asyncio.create_task(fetch(session, method, endpoint, payload, semaphore))
                tasks.append(task)
                
                # If we've hit the rate limit, sleep for a second
                rate_limit_time = None
                if no_of_request_per_sec:
                    rate_limit_time = no_of_request_per_sec 
                else:
                    if endpoint in ENDPOINT_TO_RATE_LIMIT_SEMAPHORE:
                        rate_limit_time = ENDPOINT_TO_RATE_LIMIT_SEMAPHORE.get(endpoint).get('RATE_LIMIT_IN_SECOND')
                        chunk_size = semaphore._value
                
                if rate_limit_time:
                    if (i + 1) % chunk_size == 0:
                        logger.log_debug_msg(f'Wait {rate_limit_time} to process next chunk')
                        await asyncio.sleep(rate_limit_time)
                i += 1
        else:
            for i, payload in enumerate(payload_list):
                task = asyncio.create_task(fetch(session, method, endpoint, payload, semaphore))
                tasks.append(task)

                # If we've hit the rate limit, sleep for a second
                rate_limit_time = None
                if no_of_request_per_sec:
                    rate_limit_time = no_of_request_per_sec 
                else:
                    if endpoint in ENDPOINT_TO_RATE_LIMIT_SEMAPHORE:
                        rate_limit_time = ENDPOINT_TO_RATE_LIMIT_SEMAPHORE.get(endpoint).get('RATE_LIMIT_IN_SECOND')
                
                if rate_limit_time:
                    if (i + 1) % chunk_size == 0:
                        logger.log_debug_msg(f'Wait {rate_limit_time} to process next chunk')
                        await asyncio.sleep(rate_limit_time)
        
        response_list = await asyncio.gather(*tasks, return_exceptions=True)
        logger.log_debug_msg(f'Completion of all async requests time: {time.time() - all_chunk_start_time} seconds')
        
        try:
            for response in response_list:
                if 'errorMsg' in response or 'error' in response:
                    result_dict['error_response_list'].append(response)
                else:
                    result_dict['response_list'].append(response)
        except Exception as e:
            print(e)
        
    return result_dict
        
def send_async_request(method: str, endpoint: str, payload_list: list, chunk_size: int = None, no_of_request_per_sec: float = None, loop = None):
    #response_result = asyncio.run(process_async_request(method, endpoint, payload_list, chunk_size, no_of_request_per_sec))
    payload_list_queue = ENDPOINT_TO_RATE_LIMIT_SEMAPHORE.get(endpoint).get('QUEUE') if endpoint in ENDPOINT_TO_RATE_LIMIT_SEMAPHORE else None
    if payload_list_queue:
        for payload in payload_list:
            payload_list_queue.put(payload)
    
    if loop is not None:
        logger.log_debug_msg(f'No event loop is set for {endpoint}')
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    response_result = loop.run_until_complete(process_async_request(method, endpoint, payload_list, chunk_size, no_of_request_per_sec))

    response_list = response_result['response_list']
    error_response_list = response_result['error_response_list']
    
    if len(error_response_list) > 0:
        raise Exception(f'Async requests not completed successfully, error response list: {json.dumps(error_response_list)}')
    
    return response_list