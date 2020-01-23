#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import tempfile
from datetime import datetime
import collections

from jsonschema.validators import Draft4Validator
import singer

from target_s3_json import s3
from target_s3_json import utils

logger = singer.get_logger()


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


# def persist_lines(delimiter, lines, state_file=None, bq_field_name_hook=False, bookmark_keys={}):
def persist_lines(lines, config, state_file=None):
    state = None
    stream = None
    schemas = {}
    key_properties = {}
    validators = {}

    delimiter = config.get('delimiter', '')
    include_time_suffix = config.get('include_time_suffix', True)
    bq_field_name_hook = config.get('bq_field_name_hook', False)
    bookmark_keys = config.get('bookmark_keys', {})

    filenames = []

    time_suffix = ''
    if include_time_suffix:
        now = datetime.now().strftime('%Y%m%dT%H%M%S')
        time_suffix = '-' + now

    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception(
                "Line is missing required key 'type': {}".format(line))
        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            validators[o['stream']].validate(o['record'])

            filename = o['stream'] + time_suffix + '.json'
            filename = os.path.expanduser(
                os.path.join(tempfile.gettempdir(), filename))
            if not filename in filenames:
                filenames.append(filename)

            with open(filename, 'a') as json_file:
                record = bq_hook(
                    o['record']) if bq_field_name_hook else o['record']
                json_file.write(json.dumps(record) + delimiter)

            state = None
        elif t == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
            if state_file and stream:
                save_state(state_file, stream, state, bookmark_keys)
        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o['key_properties']
        elif t == 'ACTIVATE_VERSION':
            logger.debug('ACTIVATE_VERSION message')
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

    # JSON files created uploading to S3
    for filename in filenames:
        s3.upload_file(filename, config.get('s3_bucket'),
                       config.get('s3_key_prefix'))

        # Remove the uploaded file
        os.remove(filename)

    return state


def save_state(state_file, stream, state, bookmark_keys):
    bookmark_key = bookmark_keys[stream]
    with open(state_file, 'r') as json_file:
        actual_state = json.load(json_file)
        bookmark_value = state["bookmarks"][stream][bookmark_key]
        actual_state["bookmarks"][stream][bookmark_key] = bookmark_value

    with open(state_file, 'w') as outfile:
        outfile.write(json.dumps(actual_state))


# Fields must contain only letters, numbers, and underscores, start
# with a letter or underscore, and be at most 128 characters long.
def bq_hook(obj):
    if isinstance(obj, dict):
        for key in list(obj.keys()):
            if isinstance(obj[key], list):
                for child in obj[key]:
                    bq_hook(child)
            new_key = key.replace(".", "_")
            if new_key[0].isdigit():
                new_key = "_" + new_key
            if new_key != key:
                obj[new_key] = obj[key]
                del obj[key]
    return obj


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    parser.add_argument('-s', '--state', help='State file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    config_errors = utils.validate_config(config)
    if len(config_errors) > 0:
        logger.error(
            "Invalid configuration:\n   * {}".format('\n   * '.join(config_errors)))
        exit(1)

    s3.setup_aws_client(config)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    # with open('ads_insights.json', 'r') as input:
    state = persist_lines(input,
                          config,
                          args.state,)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
