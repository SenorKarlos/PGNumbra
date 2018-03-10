#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import sys
from Queue import Queue
from threading import Thread

import requests

from pgnumbra.config import cfg_get

log = logging.getLogger(__name__)

# Last used proxy for round-robin.
last_proxy = -1

# Proxy check result constants.
check_result_ok = 0
check_result_failed = 1
check_result_banned = 2
check_result_wrong = 3
check_result_timeout = 4
check_result_exception = 5
check_result_empty = 6
check_result_max = 6  # Should be equal to maximal return code.

proxies = []


def init_proxies():
    global proxies
    proxies = check_proxies()


def have_proxies():
    return len(proxies) > 0


# Simple function to do a call to Niantic's system for
# testing proxy connectivity.
def check_proxy(proxy_queue, timeout, working_proxies, check_results):

    # Url for proxy testing.
    proxy_test_url = 'https://pgorelease.nianticlabs.com/plfe/rpc'
    proxy_test_url_ptc = 'https://sso.pokemon.com/sso/login'
    banned_status_codes = [403, 409]

    proxy = proxy_queue.get()

    check_result = check_result_ok

    if proxy and proxy[1]:

        log.debug('Checking proxy: %s', proxy[1])

        try:
            proxy_response = requests.post(proxy_test_url, '',
                                           proxies={
                                               'http': proxy[1],
                                               'https': proxy[1]
                                           },
                                           timeout=timeout,
                                           verify=False)

            proxy_response_ptc = requests.post(proxy_test_url_ptc, '',
                                           proxies={
                                               'http': proxy[1],
                                               'https': proxy[1]
                                           },
                                           timeout=timeout,
                                           verify=False)

            if ((proxy_response.status_code == 200)
                and (proxy_response_ptc.status_code == 200)):
                log.debug('Proxy %s is ok.', proxy[1])
                proxy_queue.task_done()
                working_proxies.append(proxy[1])
                check_results[check_result_ok] += 1
                return True

            elif ((proxy_response.status_code in banned_status_codes)
                or (proxy_response_ptc.status_code in banned_status_codes)):
                proxy_error = ("Proxy " + proxy[1] +
                               " is banned - got pogo status code: " +
                               str(proxy_response.status_code) + 
                               " and ptc status code " + 
                               str(proxy_response_ptc.status_code))
                check_result = check_result_banned

            else:
                proxy_error = ("Wrong status code - " +
                               str(proxy_response.status_code))
                check_result = check_result_wrong

        except requests.ConnectTimeout:
            proxy_error = ("Connection timeout (" + str(timeout) +
                           " second(s) ) via proxy " + proxy[1])
            check_result = check_result_timeout

        except requests.ConnectionError:
            proxy_error = "Failed to connect to proxy " + proxy[1]
            check_result = check_result_failed

        except Exception as e:
            proxy_error = e
            check_result = check_result_exception

    else:
        proxy_error = "Empty proxy server."
        check_result = check_result_empty

    log.warning('%s', repr(proxy_error))
    proxy_queue.task_done()

    check_results[check_result] += 1
    return False


# Check all proxies and return a working list with proxies.
def check_proxies():

    check_results = [0] * (check_result_max + 1)

    # Load proxies from the file if such a file is configured.
    proxies_file = cfg_get('proxies_file')
    if not proxies_file:
        return []

    log.info('Loading proxies from file {}.'.format(proxies_file))

    source_proxies = []

    try:
        with open(proxies_file) as f:
            for line in f:
                # Ignore blank lines and comment lines.
                if len(line.strip()) == 0 or line.startswith('#'):
                    continue
                source_proxies.append(line.strip())
    except IOError:
        log.error('Could not load proxies from {}.'.format(proxies_file))
        return []

    log.info('Loaded {} proxies.'.format(len(source_proxies)))

    if len(source_proxies) == 0:
        log.error('Proxy file {} was configured but ' +
                  'no proxies were loaded. Aborting.'.format(proxies_file))
        sys.exit(1)

    proxy_queue = Queue()
    total_proxies = len(source_proxies)

    log.info('Checking proxies...')

    working_proxies = []

    for proxy in enumerate(source_proxies):
        proxy_queue.put(proxy)

        t = Thread(target=check_proxy,
                   name='check_proxy',
                   args=(proxy_queue, 5, working_proxies, check_results))
        t.daemon = True
        t.start()

    # This is painful but we need to wait here until proxy_queue is
    # completed so we have a working list of proxies.
    proxy_queue.join()

    num_working_proxies = len(working_proxies)

    if num_working_proxies == 0:
        log.error('Proxies were configured but no working ' +
                  'proxies were found. Aborting.')
        sys.exit(1)
    else:
        other_fails = (check_results[check_result_failed] +
                       check_results[check_result_wrong] +
                       check_results[check_result_exception] +
                       check_results[check_result_empty])
        log.info('Proxy check completed. Working: %d, banned: %d, ' +
                 'timeout: %d, other fails: %d of total %d configured.',
                 num_working_proxies, check_results[check_result_banned],
                 check_results[check_result_timeout],
                 other_fails,
                 total_proxies)
        return working_proxies


# Provide new proxy
def get_new_proxy():
    if not have_proxies():
        return None
    # Simply get next proxy.
    global last_proxy
    last_proxy = (last_proxy + 1) % len(proxies)
    return proxies[last_proxy]
