#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import pywikibot
import requests
import sqlite3
import stat
import time


class Collection:
    def __init__(self):
        if not (self.db and self.name and self.properties and self.query):
            print("Please define your collection's DB, name, query and properties first.")
        self.db.cur.execute('CREATE TABLE IF NOT EXISTS %s (wikidata_id, %s)' % (self.name, ','.join(['P%s' % prop for prop in self.properties])))

    def fetch(self, filepath):
        url = 'https://wdq.wmflabs.org/api'
        if filepath and os.path.isfile(filepath) and Utils.fileage(filepath) < 3600: # Only fetch one time per hour.
            print('Fetching JSON from cache...')
            cache_file = open(filepath, 'r')
            content = cache_file.read().replace('\\\\', '\\')
            cache_file.close()
            data = json.loads(content)
        else:
            print('Fetching JSON from WikiDataQuery...')
            params = {'q': self.query, 'props': ','.join(self.properties)}
            try:
                response = requests.get(url, params=params)
                data = json.loads(response.text)
                if filepath:
                    cache_file = open(filepath, 'w')
                    cache_file.write(response.text)
                    cache_file.close()
            except Exception as e:
                data = None
                print('Fetching failed:', e)
                return
        if 'status' in data.keys() and 'items' in data['status'].keys():
            print(data['status']['items'], 'elements loaded')
            for wikidata_id in data['items']:
                self.db.cur.execute('INSERT OR IGNORE INTO %s (wikidata_id) VALUES (?)' % (self.name,), (wikidata_id,))
        if 'props' in data.keys():
            for prop in self.properties:
                if prop in data['props'].keys():
                    print(len(data['props'][prop]), 'claims for property', prop)
                    for (wikidata_id, dtype, value) in data['props'][prop]:
                        self.db.cur.execute('UPDATE %s SET P%s = ? WHERE wikidata_id = ?' % (self.name, prop), (value, wikidata_id))
                else:
                    print('No claim for property', prop)
            self.db.con.commit()

class Database:
    def __init__(self, filepath):
        self.con = sqlite3.connect(filepath)
        self.cur = self.con.cursor()

class PYWB:
    def __init__(self):
        pass

class Utils:
    @staticmethod
    def fileage(filepath):
        return time.time() - os.stat(filepath)[stat.ST_MTIME]