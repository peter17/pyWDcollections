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
        self.db.cur.execute('CREATE TABLE IF NOT EXISTS %s (wikidata_id, %s, date_time, CONSTRAINT `unique_item` UNIQUE(wikidata_id) ON CONFLICT REPLACE)' % (self.name, ','.join(['P%s' % prop for prop in self.properties])))
        self.db.cur.execute('CREATE TABLE IF NOT EXISTS interwiki (wikidata_id, lang, title, date_time, CONSTRAINT `unique_link` UNIQUE(wikidata_id, lang) ON CONFLICT REPLACE)')
        self.db.cur.execute('CREATE TABLE IF NOT EXISTS harvested (wikidata_id, %s, source, date_time, CONSTRAINT `unique_item` UNIQUE(wikidata_id, source) ON CONFLICT REPLACE)' % (','.join(['P%s' % prop for prop in self.properties])))

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
                self.db.cur.execute('INSERT OR IGNORE INTO %s (wikidata_id, date_time) VALUES (?, datetime("NOW"))' % (self.name,), (wikidata_id,))
        if 'props' in data.keys():
            for prop in self.properties:
                if prop in data['props'].keys():
                    print(len(data['props'][prop]), 'claims for property', prop)
                    for (wikidata_id, dtype, value) in data['props'][prop]:
                        self.db.cur.execute('UPDATE %s SET P%s = ? WHERE wikidata_id = ?' % (self.name, prop), (value, wikidata_id))
                else:
                    print('No claim for property', prop)
            self.db.con.commit()

    def harvest_templates(self, pywb):
        for site_id in self.templates.keys():
            searched_templates = self.templates[site_id]
            props = []
            for name in searched_templates.keys():
                params = searched_templates[name]
                for param in params.keys():
                    props.append(format(params[param]))
            print('Will harvest properties', ', '.join(props), 'from', site_id)
            self.db.cur.execute('SELECT w.wikidata_id, i.title FROM %s w LEFT JOIN interwiki i ON w.wikidata_id = i.wikidata_id WHERE lang = "%s" AND (%s)' % (self.name, site_id, ' OR '.join(['P%s IS NULL' % prop for prop in props])))
            results = self.db.cur.fetchall()
            site = pywikibot.Site(site_id.replace('wiki', ''))
            i = 0
            t = len(results)
            for (wikidata_id, title) in results:
                i += 1
                page = pywikibot.Page(site, title)
                page_templates = page.templatesWithParams()
                j = 0
                k = 0
                for template in page_templates:
                    template_name = template[0].title(withNamespace=False)
                    if template_name in searched_templates.keys():
                        j += 1
                        for param in template[1]:
                            try:
                                key = param.split('=')[0].strip()
                                val = param.split('=')[1].strip()
                                if key in searched_templates[template_name].keys() and len(val) > 2:
                                    self.db.cur.execute('INSERT OR IGNORE INTO harvested (wikidata_id, source) VALUES (?, ?)', (wikidata_id, site_id))
                                    self.db.cur.execute('UPDATE harvested SET P%s = ? WHERE wikidata_id = ? AND source = ?' % searched_templates[template_name][key], (val, wikidata_id, site_id))
                                    k += 1
                            except:
                                print('[EEE] Error when parsing "%s"' % title)
                self.db.con.commit()
                print('(%s/%s) - %s matching templates - %s values harvested in "%s"' % (i, t, j, k, title))

    def populate_interwikis(self, pywb):
        self.db.cur.execute('SELECT wikidata_id FROM %s' % (self.name,))
        results = self.db.cur.fetchall()
        i = 0
        t = len(results)
        for (wikidata_id,) in results:
            i += 1
            item = pywb.ItemPage(wikidata_id)
            if item.exists():
                print('(%s/%s) Q%s: %i interwiki' % (i, t, wikidata_id, len(item.sitelinks)))
                for lang in item.sitelinks.keys():
                    title = item.sitelinks[lang]
                    self.db.cur.execute('INSERT OR REPLACE INTO interwiki (wikidata_id, lang, title, date_time) VALUES (?, ?, ?, datetime("NOW"))', (wikidata_id, lang, title))
            if i % 50 == 0:
                self.db.con.commit()
        self.db.con.commit()

    def copy_ciwiki_to_declaration(self, pywb):
        self.db.cur.execute('SELECT wikidata_id, title FROM interwiki WHERE lang = "commonswiki" AND wikidata_id IN (SELECT wikidata_id FROM %s WHERE P373 IS NULL)' % (self.name,))
        results = self.db.cur.fetchall()
        i = 0
        t = len(results)
        for (wikidata_id, title) in results:
            i += 1
            print('(%s/%s)' % (i, t), end=' ')
            pywb.write_prop_373(wikidata_id, title)

    def copy_harvested_images(self, pywb):
        self.db.cur.execute('SELECT wikidata_id, P18, source FROM harvested WHERE P18 IS NOT NULL AND wikidata_id IN (SELECT wikidata_id FROM %s WHERE P18 IS NULL)' % (self.name,))
        results = self.db.cur.fetchall()
        i = 0
        t = len(results)
        for (wikidata_id, title, source) in results:
            i += 1
            print('(%s/%s)' % (i, t), end=' ')
            pywb.write_prop_18(wikidata_id, title)
            self.db.cur.execute('UPDATE harvested SET P18 = NULL WHERE wikidata_id = ? AND source = ?', (wikidata_id, source))
            if i % 50 == 0:
                self.db.con.commit()
        self.db.con.commit()

class Database:
    def __init__(self, filepath):
        self.con = sqlite3.connect(filepath)
        self.cur = self.con.cursor()

class PYWB:
    def __init__(self, user, lang):
        self.user = user
        self.site = pywikibot.Site(lang)
        self.commons = self.site.image_repository()
        self.wikidata = self.site.data_repository()

    def ItemPage(self, wikidata_id):
        datapage = pywikibot.ItemPage(self.wikidata, 'Q%s' % wikidata_id)
        if datapage.isRedirectPage():
            datapage = pywikibot.ItemPage(self.wikidata, datapage.getRedirectTarget().title())
        return datapage

    def Category(self, title):
        category = pywikibot.Category(self.commons, 'Category:%s' % title)
        if category.isCategoryRedirect():
            category = category.getCategoryRedirectTarget()
        return category

    def Claim(self, prop):
        return pywikibot.Claim(self.wikidata, prop)

    def FilePage(self, title):
        filepage = pywikibot.FilePage(self.commons, 'File:%s' % title)
        if filepage.isRedirectPage():
            filepage = self.FilePage(filepage.getRedirectTarget().title(withNamespace=False))
        return filepage

    def write_prop_18(self, wikidata_id, title):
        print('Q%s' % (wikidata_id), end='')
        if not title.lower().endswith('jpg'):
            print(' - Not a picture. Ignored.')
        item = self.ItemPage(wikidata_id)
        if item.exists():
            if item.claims and 'P18' in item.claims:
                print(' - Image already present.')
            else:
                title = title.replace('File:', '').replace('file:', '').strip().replace('::', ':')
                if title == '':
                    print(' - no name')
                    return
                filepage = self.FilePage(title)
                print(' -', filepage.title(withNamespace=False), end='')
                if filepage.exists():
                    claim = self.Claim('P18')
                    try:
                        claim.setTarget(filepage)
                    except:
                        print(' - wrong image "%s"' % (title,))
                    if self.wikidata.logged_in() == True and self.wikidata.user() == self.user:
                        item.addClaim(claim)
                        print(' - added!')
                    else:
                        print(' - error, please check you are logged in!')
                else:
                    print(' - image does not exist!')

    def write_prop_373(self, wikidata_id, title):
        print('Q%s - %s' % (wikidata_id, title), end='')
        item = self.ItemPage(wikidata_id)
        if item.exists():
            if item.claims and 'P373' in item.claims:
                print(' - Commonscat already present.')
            else:
                title = title.replace('Category:', '').replace('category:', '').strip().replace('::', ':')
                print(' -', title, end=' ')
                if title == '':
                    print(' - no name')
                    return
                commonscat = self.Category(title)
                if commonscat.exists():
                    claim = self.Claim('P373')
                    claim.setTarget(commonscat.title(withNamespace=False))
                    if self.wikidata.logged_in() == True and self.wikidata.user() == self.user:
                        item.addClaim(claim)
                        print(' - added!')
                    else:
                        print(' - error, please check you are logged in!')
                else:
                    print(' - category does not exist!')

class Utils:
    @staticmethod
    def fileage(filepath):
        return time.time() - os.stat(filepath)[stat.ST_MTIME]