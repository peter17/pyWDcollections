#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pywikibot
import sqlite3

class Database:
    def __init__(self, filepath):
        self.con = sqlite3.connect(filepath)
        self.cur = self.con.cursor()

class pywikibot:
    def __init__(self):
        pass