# Version: v1.0


# Standard imports
import os
import time
import json
import logging
from typing import Dict, List, Optional, Any


# Library imports
import requests


# Local imports


# Document API query: https://docs.dune.com/api-reference/executions/endpoint/execute-query


class DuneExtractor:
    """
    Dune Analytics API client for executing queries and retrieving results.
    """
    
    def __init__(self, api_key: str, poll_interval: int = 5):
        """
        Initialize DuneExtractor with API key.
        
        Args:
            api_key (str): Your Dune Analytics API key
            poll_interval (int): Polling interval in seconds (default: 5)
        """
        self.api_key = api_key
        self.poll_interval = poll_interval
        
        # Set up headers
        self.headers = {
            'X-Dune-API-Key': self.api_key,
            'Content-Type': 'application/json'
        }
        
    def execute_query(self, query_id: int, parameters: str = None) -> str:
        """
        Execute a Dune query with optional parameters.
        
        Args:
            query_id (int): The Dune query ID to execute
            parameters (str): Query parameters as key-value pairs
            
        Returns:
            str: Execution ID for polling results
        """
        execute_url = f'https://api.dune.com/api/v1/query/{query_id}/execute'
        
        payload = {}
        if parameters:
            payload['query_parameters'] = {"date": parameters}
            
        response_execute = requests.post(execute_url, headers=self.headers, json=payload)
        response_execute.raise_for_status()
        
        execution_id = response_execute.json()['execution_id']
        print(f"Query {query_id} executed. Execution ID: {execution_id}")
        
        return execution_id
        
    def get_results(self, execution_id: str, max_wait_time: int = 300) -> List[Dict[str, Any]]:
        """
        Poll for query results until completion or timeout.
        
        Args:
            execution_id (str): The execution ID from execute_query
            max_wait_time (int): Maximum time to wait in seconds (default: 300)
            
        Returns:
            List[Dict]: Query results as list of dictionaries
        """
        results_url = f'https://api.dune.com/api/v1/execution/{execution_id}/results'
        start_time = time.time()
        
        while True:
            # Check timeout
            if time.time() - start_time > max_wait_time:
                raise TimeoutError(f"Query polling timed out after {max_wait_time} seconds")
                
            response_results = requests.get(results_url, headers=self.headers)
            response_results.raise_for_status()
            
            data = response_results.json()
            state = data.get('state')
            
            if state == 'QUERY_STATE_COMPLETED':
                print('Data crawled successfully!')
                results = data['result']['rows']
                print(f"Retrieved {len(results)} rows")
                return results
                
            elif state == 'QUERY_STATE_FAILED':
                error_msg = data.get('error', 'Unknown error')
                print('Query failed:', error_msg)
                raise RuntimeError(f"Query execution failed: {error_msg}")
                
            else:
                print(f'Waiting... (state: {state})')
                time.sleep(self.poll_interval)