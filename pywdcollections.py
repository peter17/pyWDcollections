#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import re
import pywikibot
import requests
import sqlite3
import stat
import time
import threading
import json
import hashlib
import urllib.parse
import http.client as http

from codecs import open
from SPARQLWrapper import SPARQLWrapper, JSON, SPARQLExceptions

class Collection:
    def __init__(self, pywb):
        print('Initializing...')
        self.pywb = pywb
        self.commit_frequency = self.commit_frequency if hasattr(self, 'commit_frequency') else 50 # write to the DB every 50 operations
        self.harvest_frequency = self.harvest_frequency if hasattr(self, 'harvest_frequency') else 30 # harvest a Wikipedia page every 30 days
        self.update_frequency = self.update_frequency if hasattr(self, 'update_frequency') else 3 # update Wikidata items every 3 days
        self.chunk_size = self.chunk_size if hasattr(self, 'chunk_size') else 50 # parallelize http calls by groups of 50
        self.optional_articles = self.optional_articles if hasattr(self, 'optional_articles') else False # by default, harvest only items with Wikipedia articles
        self.skip_if_recent = self.skip_if_recent if hasattr(self, 'skip_if_recent') else True # don't query Wikidata again if there is a recent cache file
        self.debug = self.debug if hasattr(self, 'debug') else False # show SPARQL & SQL queries
        self.country = self.country if hasattr(self, 'country') else None
        if not (self.db and self.name and self.properties):
            print("Please define your collection's DB, name, main_type, languages and properties first.")
            return
        for prop in self.properties:
            if prop not in PYWB.managed_properties.keys():
                print('Property %s cannot be used yet. Patches are welcome.' % (prop,))
                continue
        for wiki in self.templates.keys():
            if wiki not in PYWB.sources.keys():
                print('Wikipedia instance "%s" cannot be used yet. Add its Wikidata ID to class PYWB to use it as a source.' % (wiki,))
                return
        # FIXME adapt column type to property type + store descriptions
        self.db.cur.execute('CREATE TABLE IF NOT EXISTS `%s` (wikidata_id INT, last_modified, CONSTRAINT `unique_item` UNIQUE(wikidata_id) ON CONFLICT REPLACE)' % self.name)
        self.db.cur.execute('CREATE TABLE IF NOT EXISTS interwiki (wikidata_id INT, lang, title, last_harvested, errors, CONSTRAINT `unique_link` UNIQUE(wikidata_id, lang) ON CONFLICT REPLACE)')
        self.db.cur.execute('CREATE TABLE IF NOT EXISTS harvested (wikidata_id INT, source, date_time, CONSTRAINT `unique_item` UNIQUE(wikidata_id, source) ON CONFLICT REPLACE)')
        for prop in self.properties: # add columns for each property, if they already exist, it does nothing
            try:
                self.db.cur.execute('ALTER TABLE `%s` ADD COLUMN `P%s`' % (self.name, prop))
                self.db.cur.execute('ALTER TABLE `harvested` ADD COLUMN `P%s`' % prop)
            except sqlite3.OperationalError:
                pass
        self.db.con.commit()

    def chunks(self, l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def decode(self, string):
        return urllib.parse.unquote(string.split('/')[-1]).replace('_', ' ')

    def fetch(self):
        endpoint = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"
        sparql = SPARQLWrapper(endpoint)
        keys = [self.name, 'commonslink']
        keys.extend(['P%s' % (prop,) for prop in self.properties])
        keys.extend(['label_%s' % (lang,) for lang in self.languages])
        keys.extend(['description_%s' % (lang,) for lang in self.languages])
        keys.extend(['link_%s' % (lang,) for lang in self.languages])
        keys_str = ' '.join(['?%s' % (key,) for key in keys]) + ' ?modified'
        country_filter = ('?%s wdt:P17 wd:Q%s .' % (self.name, self.country)) if self.country else ''
        values = ('VALUES ?values {%s}' % ' '.join(['wd:Q%s' % type_ for type_ in self.main_type]) ) if isinstance(self.main_type, list) else ''
        type_ = '?values' if isinstance(self.main_type, list) else 'wd:Q%s' % self.main_type
        condition = '{ %s ?%s (wdt:P31/wdt:P279*) %s . } %s ?%s schema:dateModified ?modified ' % (values, self.name, type_, country_filter, self.name)
        optional_articles = 'OPTIONAL' if self.optional_articles else ''
        optionals = ' '.join(['OPTIONAL {?%s wdt:P%s ?P%s .}' % (self.name, prop, prop) for prop in self.properties])
        for lang in self.languages:
            optionals += ' OPTIONAL { ?%s rdfs:label ?label_%s filter (lang(?label_%s) = "%s") .}' % (self.name, lang, lang, lang)
            optionals += ' OPTIONAL { ?%s schema:description ?description_%s FILTER((LANG(?description_%s)) = "%s") . }' % (self.name, lang, lang, lang)
            optionals += ' %s { ?link_%s schema:isPartOf [ wikibase:wikiGroup "wikipedia" ] ; schema:inLanguage "%s" ; schema:about ?%s}' % (optional_articles, lang, lang, self.name)
        optionals += ' OPTIONAL { ?%s ^schema:about [ schema:isPartOf <https://commons.wikimedia.org/>; schema:name ?commonslink ] . FILTER( STRSTARTS( ?commonslink, "Category:" )) . }' % (self.name,)
        langs = ','.join(self.languages)
        query = 'PREFIX schema: <http://schema.org/> SELECT DISTINCT %s WHERE { %s %s SERVICE wikibase:label { bd:serviceParam wikibase:language "%s". } }' % (keys_str, condition, optionals, langs)
        if not os.path.exists('cache'):
            os.makedirs('cache')
        cache_file = 'cache/' + self.name + '_' + '-'.join(self.languages) + '_' + hashlib.md5(query.encode('utf-8')).hexdigest()
        if os.path.isfile(cache_file) and os.path.getmtime(cache_file) > time.time() - self.update_frequency * 24 * 3600 and os.path.getsize(cache_file) > 0:
            if self.skip_if_recent:
                print('Found recent cache "%s", skipping...' % (cache_file,))
                return
            print('Loading from "%s", please wait...' % (cache_file,))
            with open(cache_file, 'r', encoding='utf-8') as content_file:
                data = json.load(content_file)
        else:
            print('Query running, please wait...')
            if self.debug:
                print(query)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            try:
                data = sparql.query().convert()
            except urllib.error.HTTPError as e:
                data = {} # avoid memory leak
                sparql = None # avoid memory leak
                if e.code in [429, 403, 500, 502, 503, 504]:
                    print('ERROR... (%s) will retry in 60 seconds...' % (e,))
                    time.sleep(60)
                    return self.fetch() # FIXME limit nb of retries or increase time between
                else:
                    print('ERROR: %s' % (e,))
                return
            except (json.decoder.JSONDecodeError, SPARQLExceptions.EndPointInternalError, http.IncompleteRead, http.RemoteDisconnected) as e:
                data = {} # avoid memory leak
                sparql = None # avoid memory leak
                message = '%s' % (e,)
                message = message[:128] + '...' if len(message) > 128 and not self.debug else message
                print('ERROR... (%s) will retry in 60 seconds...' % (message,))
                time.sleep(60)
                return self.fetch() # FIXME limit nb of retries or increase time between
            if 'results' in data.keys():
                if self.debug:
                    print('Saving to', cache_file)
                with open(cache_file, 'w') as f:
                    json.dump(data, f)
            else:
                print('Unknown error (invalid JSON with keys "%s")' % ', '.join(data.keys()))
        if 'results' in data.keys() and 'bindings' in data['results'].keys():
            self.db.cur.execute('SELECT wikidata_id, last_modified FROM `%s`' % (self.name,))
            existing_items = {wikidata_id: date_time for (wikidata_id, date_time) in self.db.cur.fetchall()}
            t = len(data['results']['bindings'])
            if self.debug:
                print(t, 'elements loaded')
            i = 0
            for item in data['results']['bindings']:
                i += 1
                wikidata_id = int(item[self.name]['value'].split('/')[-1].replace('Q', ''))
                modified = item['modified']['value'].replace('T', ' ').replace('Z', '')
                if wikidata_id in existing_items and existing_items[wikidata_id] == modified:
                    print('(%s/%s) Q%s' % (i, t, wikidata_id), '-> continue', end='     \r')
                else:
                    print('(%s/%s) Q%s' % (i, t, wikidata_id), end='                      \r')
                    self.db.cur.execute('INSERT OR IGNORE INTO `%s` (wikidata_id, last_modified) VALUES (?, ?)' % (self.name,), (wikidata_id, modified))
                    for prop in self.properties:
                        pprop = 'P%s' % (prop,)
                        if pprop in item.keys():
                            value = item[pprop]['value']
                            if prop in PYWB.managed_properties.keys():
                                if PYWB.managed_properties[prop]['type'] == 'entity':
                                    value = self.decode(value)
                                elif PYWB.managed_properties[prop]['type'] == 'image':
                                    value = self.decode(value)
                                elif PYWB.managed_properties[prop]['type'] == 'coordinates':
                                    value = value.replace('Point(', '').replace(')', '|0').replace(' ', '|')
                            self.db.cur.execute('UPDATE `%s` SET %s = ? WHERE wikidata_id = ?' % (self.name, pprop), (value, wikidata_id))
                for lang in self.languages:
                    if 'link_' + lang in item.keys():
                        title = self.decode(item['link_' + lang]['value'])
                        siteid = lang + 'wiki'
                        self.db.cur.execute('INSERT OR IGNORE INTO interwiki (wikidata_id, lang, title, last_harvested) VALUES (?, ?, ?, NULL)', (wikidata_id, siteid, title))
            print('')
            self.commit(0)

    def find_coordinates_in_template(self, template):
        (latitude, longitude) = (None, None)
        if len(template) > 1 and len(template[1]) >= 8:
            latitude = "%s/%s/%s/%s" % (template[1][0], template[1][1], template[1][2], template[1][3])
            longitude = "%s/%s/%s/%s" % (template[1][4], template[1][5], template[1][6], template[1][7])
        elif len(template) > 1 and len(template[1]) > 1:
            latitude = format(template[1][0])
            longitude = format(template[1][1])
        return (latitude, longitude)

    def find_items_in_value(self, site, val, constraints, one = False):
        matches = re.findall('\[\[(.*?)\]\]', val, re.DOTALL)
        result = []
        for match in matches:
            value = match.strip()
            if ':' in value:
                continue # Ignore images
            page = pywikibot.Page(site, value)
            if page.exists():
                if page.isRedirectPage():
                    page = page.getRedirectTarget()
                if 'wikibase_item' in page.properties():
                    wikidata_id = page.properties()['wikibase_item']
                    if constraints and self.pywb.check_constraints(wikidata_id, constraints):
                        if one:
                            return wikidata_id
                        if wikidata_id not in result:
                            result.append(wikidata_id)
                    else:
                        result.append(wikidata_id)
        return None if one else result

    def harvest_templates(self, only_those = None):
        for site_id in (only_those if only_those else self.templates.keys()):
            searched_templates = self.templates[site_id]
            props = []
            for name in searched_templates.keys():
                params = searched_templates[name]
                if isinstance(params, dict):
                    for param in params.keys():
                        prop = format(params[param]).replace('a', '').replace('b', '')
                        if int(prop) in self.properties:
                            props.append(prop)
                elif isinstance(params, int):
                    prop = format(params).replace('a', '').replace('b', '')
                    if int(prop) in self.properties:
                        props.append(prop)
            props = list(set(props)) # remove duplicates
            print('Will harvest properties', ', '.join(props), 'from', site_id)
            query = 'SELECT w.wikidata_id, i.title, %s FROM `%s` w JOIN interwiki i ON w.wikidata_id = i.wikidata_id WHERE lang = ? AND (%s) AND ((julianday(datetime("now")) - julianday(last_harvested)) > ? OR last_harvested IS NULL)' % (','.join(['P%s' % prop for prop in props]), self.name, ' OR '.join(['P%s IS NULL' % prop for prop in props]))
            if self.debug:
                print(query)
            self.db.cur.execute(query, (site_id, self.harvest_frequency))
            results = self.db.cur.fetchall()
            t = len(results)
            print(t, 'pages to harvest.')
            if t == 0:
                continue
            pages = {}
            site = pywikibot.Site(site_id.replace('wiki', ''))
            for (wikidata_id, title, *values) in results:
                pages['Q%s' % (wikidata_id,)] = {
                    'page': pywikibot.Page(site, title),
                    'values': values,
                }
            print('Fetching %s pages (%s chunks of %s)' % (t, t // self.chunk_size, self.chunk_size))
            i = 0
            for chunk in self.chunks(list(pages.keys()), self.chunk_size):
                threads = []
                for qid in chunk:
                    thread = threading.Thread(target=PYWB.fetch_page_templates, args=(pages[qid],))
                    thread.start()
                    threads.append(thread)
                for thread in threads:
                    thread.join()
                for qid in chunk:
                    self.harvest_templates_for_page(pages[qid]['page'], site_id, int(qid.replace('Q', '')), pages[qid]['values'], props)
                    i += 1
                    print('(%s/%s)' % (i, t), end='')
                self.commit(0)
            print('Done!         ')

    def harvest_templates_for_page(self, page, site_id, wikidata_id, values, props):
        errors = []
        searched_templates = self.templates[site_id]
        title = page.title(withNamespace=False)
        props_to_analyze = {}
        for (index, prop) in enumerate(props):
            pprop = 'P%s' % (prop,)
            props_to_analyze[pprop] = values[index] == None
        j = 0
        k = 0
        for template in page.templatesWithParams():
            template_name = template[0].title(withNamespace=False)
            if template_name in searched_templates.keys():
                j += 1
                (latitude, longitude) = (None, None)
                for param in template[1]:
                    param.replace('{{PAGENAME}}', title)
                    try:
                        searched_template = searched_templates[template_name]
                        if isinstance(searched_template, dict): # template with named parameters
                            keyval = param.split('=')
                            if len(keyval) != 2:
                                continue
                            key = keyval[0].strip()
                            val = keyval[1].strip()
                            if key in searched_template.keys() and len(val) > 2:
                                searched_property = searched_template[key]
                                pprop = 'P%s' % (searched_property,)
                                searched_property = searched_property if pprop in props_to_analyze.keys() else None # avoid harvesting props that are already defined
                                if searched_property and searched_property in PYWB.managed_properties.keys() and PYWB.managed_properties[searched_property]['type'] == 'entity': # fetch wikidata_id of link target
                                    val = self.find_items_in_value(page.site, val, PYWB.managed_properties[searched_property]['constraints'], not PYWB.managed_properties[searched_property]['multiple'])
                                elif searched_property == '625a':
                                    latitude = val
                                elif searched_property == '625b':
                                    longitude = val
                                elif searched_property == 625:
                                    val = val.strip().replace('\t', '').replace(' ', '|').replace('°', '/').replace('′', '/').replace('″', '/').replace("'", '/').replace('"', '/') + '|0'
                                if searched_property in ['625a', '625b'] and latitude and longitude:
                                    searched_property = 625
                                    val = '%s|%s|0' % (latitude, longitude)
                                if format(searched_property) in props and searched_property not in ['625a','625b'] and val:
                                    self.db.cur.execute('INSERT OR IGNORE INTO harvested (wikidata_id, source) VALUES (?, ?)', (wikidata_id, site_id))
                                    self.db.cur.execute('UPDATE harvested SET P%s = ?, date_time = datetime("NOW") WHERE wikidata_id = ? AND source = ?' % searched_property, (val, wikidata_id, site_id))
                                    k += 1
                        elif isinstance(searched_template, int) and len(param) > 2: # template with single parameter
                            searched_property = searched_template
                            if searched_property == 625:
                                (latitude, longitude) = self.find_coordinates_in_template(template)
                                param = '%s|%s|0' if latitude and longitude else ''
                                self.db.cur.execute('INSERT OR IGNORE INTO harvested (wikidata_id, source) VALUES (?, ?)', (wikidata_id, site_id))
                                self.db.cur.execute('UPDATE harvested SET P%s = ? WHERE wikidata_id = ? AND source = ?' % searched_template, (param, wikidata_id, site_id))
                                k += 1
                                break # to consider only the 1st parameter (e.g. {{Commonscat|commonscat|display}}
                    except Exception as e:
                        errors.append(str(e))
                        print('[EEE] Error when parsing param "%s" in template "%s" on "%s" (%s)' % (param, template_name, title, e))
        self.db.cur.execute('UPDATE interwiki SET last_harvested = datetime("NOW"), errors = ? WHERE wikidata_id = ? AND lang = ?', (' | '.join(errors), wikidata_id, site_id))
        if self.debug:
            print(' - %s matching templates - %s values harvested in "%s"' % (j, k, title))
        else:
            print(' - %s matching templates - %s values harvested       ' % (j, k), end='\r')


    def mark_outdated(self, wikidata_id):
        self.db.cur.execute('UPDATE `%s` SET last_modified = NULL WHERE wikidata_id = ?' % (self.name,), (wikidata_id,))

    def update_item(self, item):
        i = 0
        wikidata_id = int(item.title().replace('Q', ''))
        for prop in self.properties:
            value = self.pywb.get_claim_value(prop, item)
            if value:
                i += 1
                self.db.cur.execute('UPDATE `%s` SET P%s = ? WHERE wikidata_id = ?' % (self.name, prop), (value, wikidata_id))
        self.db.cur.execute('UPDATE `%s` SET last_modified = datetime("NOW") WHERE wikidata_id = ?' % (self.name,), (wikidata_id,))
        print('- %s properties updated.' % (i,))

    def update_outdated_items(self):
        self.db.cur.execute('SELECT wikidata_id FROM `%s` WHERE last_modified IS NULL' % (self.name,))
        ids_to_update = [item[0] for item in self.db.cur.fetchall()]
        total = len(ids_to_update)
        print(total, 'elements to update.')
        i = 0
        for wikidata_id in ids_to_update:
            i += 1
            item = self.get_item(wikidata_id)
            if item and item.exists():
                print('(%s/%s) - Q%s' % (i, total, wikidata_id), end=' ')
                self.update_item(item)
            self.commit(i)
        self.commit(0)

    def get_item(self, wikidata_id):
        item = self.pywb.ItemPage(wikidata_id)
        new_id = int(item.title().replace('Q', ''))
        # If id has changed (item is a redirect), update to new one.
        if new_id != wikidata_id:
            self.db.cur.execute('SELECT wikidata_id FROM `%s` WHERE wikidata_id = ?' % (self.name,), (new_id,))
            if len(self.db.cur.fetchall()) == 0: # avoid unicity constraint violation
                self.db.cur.execute('UPDATE `%s` SET wikidata_id = ? WHERE wikidata_id = ?' % (self.name,), (new_id, wikidata_id))
            else:
                self.db.cur.execute('DELETE FROM `%s` WHERE wikidata_id = ?' % (self.name,), (wikidata_id,))
                return None
        return item

    def commit(self, count):
        # Autocommit every N operations. Or now if count = 0.
        if count % self.commit_frequency == 0:
            self.db.con.commit()

    def copy_harvested_properties(self, props):
        for prop in props:
            print('Will write harvested P%s' % (prop))
            self.copy_harvested_property(prop)

    def copy_harvested_property(self, prop):
        query = 'SELECT wikidata_id, P%s, source FROM harvested WHERE P%s IS NOT NULL AND wikidata_id IN (SELECT wikidata_id FROM `%s` WHERE P%s IS NULL)' % (prop, prop, self.name, prop)
        if self.debug:
            print(query)
        self.db.cur.execute(query)
        results = self.db.cur.fetchall()
        i = 0
        t = len(results)
        print('Found %s values to write for P%s.' % (t, prop))
        if t > 0:
            self.pywb.wikidata.login()
        for (wikidata_id, title, source) in results:
            i += 1
            print('(%s/%s)' % (i, t), end=' ')
            self.pywb.write_prop(prop, wikidata_id, title, source)
            self.mark_outdated(wikidata_id)
            self.db.cur.execute('UPDATE harvested SET P%s = NULL WHERE wikidata_id = ? AND source = ?' % (prop,), (wikidata_id, source))
            self.commit(i)
        self.commit(0)

    def copy_ciwiki_to_declaration(self):
        self.db.cur.execute('SELECT wikidata_id, title FROM interwiki WHERE lang = "commonswiki" AND wikidata_id IN (SELECT wikidata_id FROM `%s` WHERE P373 IS NULL)' % (self.name,))
        results = self.db.cur.fetchall()
        i = 0
        t = len(results)
        if t > 0:
            self.pywb.wikidata.login()
        for (wikidata_id, title) in results:
            i += 1
            print('(%s/%s)' % (i, t), end=' ')
            self.pywb.write_prop_373(wikidata_id, title)
            self.mark_outdated(wikidata_id)
            self.commit(i)
        self.commit(0)


class Database:
    def __init__(self, filepath):
        self.con = sqlite3.connect(filepath)
        self.cur = self.con.cursor()

class PYWB:
    managed_properties = {
	17: { 'type': 'entity', 'constraints': [3624078, 6256], 'multiple': False },
	18: { 'type': 'image' },
	31: { 'type': 'entity', 'constraints': [], 'multiple': False },
	131: { 'type': 'entity', 'constraints': [515, 1549591, 56061], 'multiple': False },
	373: { 'type': 'string' },
	380: { 'type': 'string' },
	625: { 'type': 'coordinates' },
	708: { 'type': 'entity', 'constraints': [285181, 620225, 2288631, 1531518, 1778235, 1431554, 384003, 3146899, 665487, 3732788], 'multiple': False },
	856: { 'type': 'string' },
	1435: { 'type': 'string' },
	1644: { 'type': 'string' },
	1866: { 'type': 'string' },
	2971: { 'type': 'integer' },
    }
    sources = {
	'alswiki': 1211233,
	'arwiki': 199700,
	'arzwiki': 2374285,
	'azwiki': 58251,
	'azbwiki': 20789766,
	'bawiki': 58209,
	'banwiki': 70885480,
	'bewiki': 877583,
	'bgwiki': 11913,
	'bhwiki': 8561277,
	'bnwiki': 427715,
	'bswiki': 1047829,
	'cawiki': 199693,
	'dawiki': 181163,
	'dewiki': 48183,
	'dtywiki': 29048035,
	'elwiki': 11918,
	'enwiki': 328,
	'eswiki': 8449,
	'eowiki': 190551,
	'euwiki': 207260,
	'fawiki': 48952,
	'frwiki': 8447,
	'gagwiki': 79633,
	'glwiki': 841208,
	'guwiki': 3180306,
	'hiwiki': 722040,
	'hrwiki': 203488,
	'huwiki': 53464,
	'hywiki': 1975217,
	'hywwiki': 60437959,
	'idwiki': 155214,
	'itwiki': 11920,
	'jawiki': 177837,
	'jvwiki': 3477935,
	'kawiki': 848974,
	'kkwiki': 58172,
	'kowiki': 17985,
	'lbwiki': 950058,
	'nlwiki': 10000,
	'ocwiki': 595628,
	'plwiki': 1551807,
	'ptwiki': 11921,
	'rowiki': 199864,
	'ruwiki': 206855,
	'urwiki': 1067878,
	'zhwiki': 30239,
    }

    def __init__(self, user, lang):
        self.user = user
        self.site = pywikibot.Site(lang)
        self.commons = self.site.image_repository()
        self.wikidata = self.site.data_repository()

    def ItemPage(self, wikidata_id):
        datapage = pywikibot.ItemPage(self.wikidata, wikidata_id if format(wikidata_id).startswith('Q') else 'Q%s' % wikidata_id)
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

    def Coordinate(self, latitude, longitude):
        return pywikibot.Coordinate(latitude, longitude, dim=10, site=self.wikidata)

    def FilePage(self, title):
        filepage = pywikibot.FilePage(self.commons, 'File:%s' % title)
        if filepage.isRedirectPage():
            filepage = self.FilePage(filepage.getRedirectTarget().title(withNamespace=False))
        return filepage

    def addClaim(self, item, claim, source = None):
        if self.wikidata.logged_in() == True and self.wikidata.user() == self.user:
            item.addClaim(claim)
            if source and source in self.sources.keys():
                sourceItem = self.ItemPage(self.sources[source])
                qualifier = self.Claim('P143')
                qualifier.setTarget(sourceItem)
                claim.addSource(qualifier)
            print(' - added!')
        else:
            print(' - error, please check you are logged in!')

    def check_constraints(self, wikidata_id, constraints):
        item = self.ItemPage(wikidata_id) # FIXME cache those items
        if item.exists():
            claims = item.claims or {}
            if 'P31' in claims:
                for claim in claims['P31']:
                    nature = claim.getTarget().title().replace('Q', '') if claim.getTarget() else ''
                    if int(nature) in constraints:
                        return item
        return False

    @staticmethod
    def fetch_page_templates(page):
        page['page'].templatesWithParams()

    def get_claim_value(self, prop, item):
        claims = item.claims if item.claims else {}
        pprop = 'P%s' % (prop,)
        if pprop in claims and prop in self.managed_properties.keys():
            if self.managed_properties[prop]['type'] in ['entity', 'image']:
                return claims[pprop][0].getTarget().title(withNamespace=False) if claims[pprop][0].getTarget() else ''
            elif self.managed_properties[prop]['type'] == 'string':
                return claims[pprop][0].getTarget()
            elif self.managed_properties[prop]['type'] == 'coordinates':
                target = claims[pprop][0].getTarget()
                return '%f|%f|%f' % (float(target.lat), float(target.lon), float(target.alt if target.alt else 0))
        return None

    def write_prop(self, prop, wikidata_id, value, source = None):
        if prop == 17:
            return self.write_prop_item(prop, wikidata_id, value, source)
        elif prop == 18:
            return self.write_prop_18(wikidata_id, value, source)
        elif prop == 131:
            return self.write_prop_item(prop, wikidata_id, value, source)
        elif prop == 373:
            return self.write_prop_373(wikidata_id, value, source)
        elif prop == 625:
            return self.write_prop_625(wikidata_id, value, source)
        print('Writing prop %s is not implemented yet! Patches are welcome!')
        return False

    def write_prop_item(self, prop, wikidata_id, value, source = None):
        print('Q%s' % (wikidata_id), end='')
        target = self.check_constraints(value, PYWB.managed_properties[prop]['constraints'])
        if not target:
            print(' - Constraints not matched. Ignored.')
            return
        item = self.ItemPage(wikidata_id)
        if item.exists():
            pprop = 'P%s' % (prop,)
            if item.claims and pprop in item.claims:
                print(' -', pprop, 'already present.')
            else:
                claim = self.Claim(pprop)
                try:
                    claim.setTarget(target)
                except:
                    print(' - problem with "%s"' % (title,))
                self.addClaim(item, claim, source)

    def write_prop_18(self, wikidata_id, title, source = None):
        print('Q%s' % (wikidata_id), end='')
        if not title.lower().endswith(('jpg', 'jpeg')):
            print(' - Not a picture. Ignored.')
            return
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
                    self.addClaim(item, claim, source)
                else:
                    print(' - image does not exist!')

    def write_prop_373(self, wikidata_id, title, source = None):
        print('Q%s - %s' % (wikidata_id, title), end='')
        item = self.ItemPage(wikidata_id)
        if item.exists():
            if item.claims and 'P373' in item.claims:
                print(' - Commonscat already present.')
            else:
                title = title.replace('Category:', '').replace('category:', '').strip().replace('::', ':').replace('{', '').replace('}', '').replace('[', '').replace(']', '')
                print(' -', title, end=' ')
                if title == '':
                    print(' - no name')
                    return
                commonscat = self.Category(title)
                if commonscat.exists():
                    claim = self.Claim('P373')
                    claim.setTarget(commonscat.title(withNamespace=False))
                    self.addClaim(item, claim, source)
                else:
                    print(' - category does not exist!')

    def write_prop_625(self, wikidata_id, coords, source = None):
        print('Q%s - %s' % (wikidata_id, coords), end='')
        item = self.ItemPage(wikidata_id)
        if item.exists():
            if item.claims and 'P625' in item.claims:
                print(' - Coordinates already present.')
            else:
                coordinates = coords.split('|')
                print(' -', coordinates, end=' ')
                if len(coordinates) != 3:
                    print(' - invalid coordinates')
                    return
                claim = self.Claim('P625')
                latitude = coordinates[0]
                longitude = coordinates[1]
                try:
                    latitude = float(latitude)
                    longitude = float(longitude)
                except:
                    try:
                        parts = latitude.split('/')
                        latitude = round(int(parts[0]) + int(parts[1]) / 60 + float(parts[2]) / 3600, 5)
                        assert parts[3] in ['N', 'S']
                        if parts[3] == 'S':
                            latitude *= -1
                        parts = longitude.split('/')
                        longitude = round(int(parts[0]) + int(parts[1]) / 60 + float(parts[2]) / 3600, 5)
                        assert parts[3] in ['E', 'W']
                        if parts[3] == 'W':
                            longitude *= -1
                    except:
                        print('- wrong format!')
                        return
                coordinates = self.Coordinate(latitude, longitude)
                claim.setTarget(coordinates)
                self.addClaim(item, claim, source)

    def write_prop_2971(self, wikidata_id, gcatholic_id, source = None):
        print('Q%s - %s' % (wikidata_id, gcatholic_id), end='')
        item = self.ItemPage(wikidata_id)
        if item.exists():
            if item.claims and 'P2971' in item.claims:
                print(' - GCatholic church ID already present.')
            else:
                try:
                    int(gcatholic_id)
                except:
                    print('- wrong format!')
                    return
                claim = self.Claim('P2971')
                claim.setTarget(gcatholic_id)
                self.addClaim(item, claim, source)
