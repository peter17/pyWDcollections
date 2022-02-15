#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import re
import pywikibot
import sqlite3
import time
import threading
import hashlib
import urllib.parse
import http.client as http

from codecs import open
from SPARQLWrapper import SPARQLWrapper, JSON, SPARQLExceptions

class Collection:
    def __init__(self, pywb):
        print('Checking configuration...', end=' ')
        self.pywb = pywb
        self.commit_frequency = self.commit_frequency if hasattr(self, 'commit_frequency') else 50 # write to the DB every 50 operations
        self.harvest_frequency = self.harvest_frequency if hasattr(self, 'harvest_frequency') else 30 # harvest a Wikipedia page every 30 days
        self.update_frequency = self.update_frequency if hasattr(self, 'update_frequency') else 3 # update Wikidata items every 3 days
        self.chunk_size = self.chunk_size if hasattr(self, 'chunk_size') else 50 # parallelize http calls by groups of 50
        # FIXME optional_articles False means there MUST be an article in EACH language, that's wrong, we should require AT LEAST one article among all the languages
        self.optional_articles = self.optional_articles if hasattr(self, 'optional_articles') else False # by default, harvest only items with Wikipedia articles
        self.skip_if_recent = self.skip_if_recent if hasattr(self, 'skip_if_recent') else True # don't query Wikidata again if there is a recent cache file
        self.debug = self.debug if hasattr(self, 'debug') else False # show SPARQL & SQL queries
        self.country = self.country if hasattr(self, 'country') else None
        self.save_texts = False # save labels and descriptions in the local database
        if not (self.db and self.name and self.properties):
            print("Please define your collection's DB, name, main_type, languages and properties first.")
            return
        for prop in self.properties:
            if prop not in PYWB.managed_properties:
                print('Property %s cannot be used yet. Patches are welcome.' % (prop,))
        for wiki in self.templates.keys():
            if wiki not in PYWB.sources:
                print('Wikipedia instance "%s" cannot be used yet. Add its Wikidata ID to class PYWB to use it as a source.' % (wiki,))
                return
        # FIXME adapt column type to property type + store descriptions
        self.db.cur.execute('CREATE TABLE IF NOT EXISTS `%s` (wikidata_id INT, last_modified, CONSTRAINT `unique_item` UNIQUE(wikidata_id) ON CONFLICT REPLACE)' % self.name)
        self.db.cur.execute('CREATE TABLE IF NOT EXISTS interwiki (wikidata_id INT, lang, title, last_harvested, errors, CONSTRAINT `unique_link` UNIQUE(wikidata_id, lang) ON CONFLICT REPLACE)')
        self.db.cur.execute('CREATE TABLE IF NOT EXISTS harvested (wikidata_id INT, source, date_time, CONSTRAINT `unique_item` UNIQUE(wikidata_id, source) ON CONFLICT REPLACE)')
        self.db.cur.execute('CREATE TABLE IF NOT EXISTS texts (wikidata_id INT, lang, label, description, CONSTRAINT `unique_language` UNIQUE(wikidata_id, lang) ON CONFLICT REPLACE)')
        for prop in self.properties: # add columns for each property, if they already exist, it does nothing
            try:
                self.db.cur.execute('ALTER TABLE `%s` ADD COLUMN `P%s`' % (self.name, prop))
                self.db.cur.execute('ALTER TABLE `harvested` ADD COLUMN `P%s`' % prop)
            except sqlite3.OperationalError:
                pass
        self.db.con.commit()
        print('done!')

    @staticmethod
    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    @staticmethod
    def decode(string):
        return urllib.parse.unquote(string.split('/')[-1]).replace('_', ' ')

    def fetch(self):
        languages = sorted(self.languages) # ensure same query to allow caching
        properties = sorted(self.properties)
        endpoint = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"
        sparql = SPARQLWrapper(endpoint)
        keys = [self.name, 'commonslink']
        keys.extend(['P%s' % (prop,) for prop in properties])
        keys.extend(['label_%s' % (lang,) for lang in languages])
        keys.extend(['description_%s' % (lang,) for lang in languages])
        keys.extend(['link_%s' % (lang,) for lang in languages])
        keys_str = ' '.join(['?%s' % (key,) for key in keys]) + ' ?modified'
        country_filter = ('?%s wdt:P17 wd:Q%s .' % (self.name, self.country)) if self.country else ''
        main_condition = ' (wdt:P31/wdt:P279*) wd:Q%s ' % self.main_type if self.main_type else self.main_condition
        condition = '{ ?%s %s . } %s ?%s schema:dateModified ?modified ' % (self.name, main_condition, country_filter, self.name)
        optional_articles = 'OPTIONAL' if self.optional_articles else ''
        optionals = ' '.join(['OPTIONAL {?%s wdt:P%s ?P%s .}' % (self.name, prop, prop) for prop in properties])
        for lang in languages:
            optionals += ' OPTIONAL { ?%s rdfs:label ?label_%s filter (lang(?label_%s) = "%s") .}' % (self.name, lang, lang, lang)
            optionals += ' OPTIONAL { ?%s schema:description ?description_%s FILTER((LANG(?description_%s)) = "%s") . }' % (self.name, lang, lang, lang)
            optionals += ' %s { ?link_%s schema:isPartOf [ wikibase:wikiGroup "wikipedia" ] ; schema:inLanguage "%s" ; schema:about ?%s}' % (optional_articles, lang, lang, self.name)
        optionals += ' OPTIONAL { ?%s ^schema:about [ schema:isPartOf <https://commons.wikimedia.org/>; schema:name ?commonslink ] . FILTER( STRSTARTS( ?commonslink, "Category:" )) . }' % (self.name,)
        langs = ','.join(languages)
        query = 'PREFIX schema: <http://schema.org/> SELECT DISTINCT %s WHERE { %s %s SERVICE wikibase:label { bd:serviceParam wikibase:language "%s". } }' % (keys_str, condition, optionals, langs)
        if not os.path.exists('cache'):
            os.makedirs('cache')
        cache_file = 'cache/' + self.name + '_' + '-'.join(languages) + '_' + hashlib.md5(query.encode('utf-8')).hexdigest()
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
                    e = None # avoid memory leak
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
            existing_items = dict(self.db.cur.fetchall())
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
                            if prop in PYWB.managed_properties:
                                if PYWB.managed_properties[prop]['type'] in ['entity', 'image', 'sound']:
                                    value = self.decode(value)
                                elif PYWB.managed_properties[prop]['type'] == 'coordinates':
                                    values = value.replace('Point(', '').replace(')', '').split(' ')
                                    value = '%s|%s|0' % (values[1], values[0]) if len(values) == 2 else ''
                            self.db.cur.execute('UPDATE `%s` SET %s = ? WHERE wikidata_id = ?' % (self.name, pprop), (value, wikidata_id))
                for lang in self.languages:
                    if 'link_' + lang in item.keys():
                        title = self.decode(item['link_' + lang]['value'])
                        siteid = lang + 'wiki'
                        self.db.cur.execute('INSERT INTO interwiki (wikidata_id, lang, title, last_harvested) VALUES (?, ?, ?, NULL) ON CONFLICT (wikidata_id, lang) DO UPDATE SET title = ?', (wikidata_id, siteid, title, title))
                if 'commonslink' in item.keys():
                    title = item['commonslink']['value']
                    self.db.cur.execute('INSERT INTO interwiki (wikidata_id, lang, title, last_harvested) VALUES (?, "commonswiki", ?, NULL) ON CONFLICT (wikidata_id, lang) DO UPDATE SET title = ?', (wikidata_id, title, title))
                for lang in self.languages:
                    label = item.get('label_' + lang, {}).get('value', '')
                    description = item.get('description_' + lang, {}).get('value', '')
                    self.db.cur.execute('INSERT INTO texts (wikidata_id, lang, label, description) VALUES (?, ?, ?, ?) ON CONFLICT (wikidata_id, lang) DO UPDATE SET label = ?, description = ?', (wikidata_id, lang, label, description, label, description))
            print('')
            self.commit(0)

    @staticmethod
    def find_coordinates_in_template(template):
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
            site_id = site.lang + 'wiki'
            page = self.pywb.Page(site_id, value)
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
        return result[0] if len(result) == 1 and one else result if not one else None

    def list_props_for_site_id(self, site_id):
        props = []
        for name, params in self.templates[site_id].items():
            if isinstance(params, dict):
                for param in params.keys():
                    prop = format(params[param]).replace('a', '').replace('b', '')
                    if int(prop) in self.properties:
                        props.append(prop)
            elif isinstance(params, int):
                prop = format(params)
                if int(prop) in self.properties:
                    props.append(prop)
        return list(set(props)) # remove duplicates

    def debug_templates(self, site_id, title):
        props = self.list_props_for_site_id(site_id)
        print('Will harvest properties', ', '.join(props), 'from', site_id, 'on', title)
        query = 'SELECT w.wikidata_id, i.title, %s FROM `%s` w JOIN interwiki i ON w.wikidata_id = i.wikidata_id WHERE lang = ? AND title = ?' % (','.join(['P%s' % prop for prop in props]), self.name)
        if self.debug:
            print(query)
        self.db.cur.execute(query, (site_id, title))
        results = self.db.cur.fetchall()
        for (wikidata_id, title, *values) in results:
            self.harvest_templates_for_page(self.pywb.Page(site_id, title), site_id, wikidata_id, values, props)

    def harvest_templates(self, only_those = None):
        for site_id in (only_those if only_those else self.templates.keys()):
            props = self.list_props_for_site_id(site_id)
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
            for (wikidata_id, title, *values) in results:
                pages['Q%s' % (wikidata_id,)] = {
                    'page': self.pywb.Page(site_id, title),
                    'values': values,
                }
            nb_chunks = t // self.chunk_size + (1 if (t // self.chunk_size) * self.chunk_size < t else 0)
            print('Fetching %s pages (%s chunk%s of %s)' % (t, nb_chunks, 's' if nb_chunks > 1 else '', self.chunk_size))
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

    @staticmethod
    def copy_with_lowercase_keys(original):
        copy = {}
        for name, params in original.items():
            if isinstance(params, dict):
                value = {}
                for param, prop in params.items():
                    value[param.lower()] = prop
            elif isinstance(params, int):
                value = params
            copy[name.lower()] = value
        return copy

    def get_template_name_with_redirect(self, site_id, template_page):
        template_name = template_page.title(with_ns=False).lower()
        if site_id in self.pywb.pages.keys() and template_name in self.pywb.pages[site_id].keys():
            return self.pywb.pages[site_id][template_name]
        if template_page.isRedirectPage():
            template_page = template_page.getRedirectTarget()
            template_name = template_page.title(with_ns=False).lower()
        if site_id not in self.pywb.pages.keys():
            self.pywb.pages[site_id] = {}
        self.pywb.pages[site_id][template_name] = template_name
        return template_name

    def harvest_templates_for_page(self, page, site_id, wikidata_id, values, props):
        errors = []
        searched_templates = self.copy_with_lowercase_keys(self.templates[site_id])
        title = page.title(with_ns=False)
        props_to_analyze = {}
        for (index, prop) in enumerate(props):
            pprop = 'P%s' % (prop,)
            props_to_analyze[pprop] = values[index] is None
        j = 0
        k = 0
        for template in page.templatesWithParams():
            template_page = template[0]
            template_name = self.get_template_name_with_redirect(site_id, template_page)
            if template_name in searched_templates:
                j += 1
                searched_template = searched_templates[template_name]
                (latitude, longitude) = (None, None)
                for param in template[1]:
                    param.replace('{{PAGENAME}}', title)
                    try:
                        if isinstance(searched_template, dict): # template with named parameters
                            keyval = param.split('=')
                            if len(keyval) != 2:
                                continue
                            key = keyval[0].strip().lower()
                            val = keyval[1].strip()
                            if key in searched_template.keys() and len(val) > 2:
                                searched_property = searched_template[key]
                                pprop = 'P%s' % (searched_property,)
                                searched_property = searched_property if pprop in props_to_analyze else None # avoid harvesting props that are already defined
                                if searched_property and searched_property in PYWB.managed_properties and PYWB.managed_properties[searched_property]['type'] == 'entity': # fetch wikidata_id of link target
                                    val = self.find_items_in_value(page.site, val, PYWB.managed_properties[searched_property]['constraints'], not PYWB.managed_properties[searched_property]['multiple'])
                                elif searched_property == '625a':
                                    latitude = val
                                elif searched_property == '625b':
                                    longitude = val
                                elif searched_property == 625:
                                    val = val.strip().replace('\t', '').replace(' ', '|')
                                    if val.count('/') == 1:
                                        val = val.replace('/', '|') + '|0'
                                    else:
                                        val = val.replace('°', '/').replace('′', '/').replace('″', '/').replace("'", '/').replace('"', '/').replace('N/', 'N|').replace('S/', 'S|') + '|0'
                                if searched_property in ['625a', '625b'] and latitude and longitude:
                                    searched_property = 625
                                    val = '%s|%s|0' % (latitude, longitude)
                                if format(searched_property) in props and searched_property not in ['625a','625b'] and val:
                                    self.save_harvested_value(searched_property, val, wikidata_id, site_id)
                                    k += 1
                        elif isinstance(searched_template, int) and len(param) > 2: # template with single parameter
                            searched_property = searched_template
                            if searched_property == 625:
                                (latitude, longitude) = self.find_coordinates_in_template(template)
                                param = '%s|%s|0' if latitude and longitude else ''
                            self.save_harvested_value(searched_template, param, wikidata_id, site_id)
                            k += 1
                            break # to consider only the 1st parameter (e.g. {{Commonscat|commonscat|display}}
                    except Exception as e:
                        errors.append(str(e))
                        print('[EEE] Error when parsing param "%s" in template "%s" on "%s" (%s)' % (param, template_name, title, e))
        self.db.cur.execute('UPDATE interwiki SET last_harvested = datetime("NOW"), errors = ? WHERE wikidata_id = ? AND lang = ?', (' | '.join(errors), wikidata_id, site_id))
        if self.debug:
            if errors:
                print('Errors:')
                for error in errors:
                    print(error)
            print(' - %s matching templates - %s values harvested in "%s"' % (j, k, title))
        else:
            print(' - %s matching templates - %s values harvested       ' % (j, k), end='\r')

    def save_harvested_value(self, searched_property, value, wikidata_id, site_id):
        if self.debug:
            print('Saving value', value, 'for property', searched_property, 'for', wikidata_id, 'and', site_id)
        self.db.cur.execute('INSERT OR IGNORE INTO harvested (wikidata_id, source) VALUES (?, ?)', (wikidata_id, site_id))
        self.db.cur.execute('UPDATE harvested SET P%s = ? WHERE wikidata_id = ? AND source = ?' % searched_property, (value, wikidata_id, site_id))

    def mark_outdated(self, wikidata_id):
        self.db.cur.execute('UPDATE `%s` SET last_modified = NULL WHERE wikidata_id = ?' % (self.name,), (wikidata_id,))

    def update_item(self, item): # FIXME update sitelinks, labels & descriptions
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
            try:
                if item and item.exists():
                    print('(%s/%s) - Q%s' % (i, total, wikidata_id), end=' ')
                    self.update_item(item)
                else:
                    self.db.cur.execute('DELETE FROM `%s` WHERE wikidata_id = ?' % (self.name,), (wikidata_id,))
            except pywikibot.exceptions.MaxlagTimeoutError as e:
                print('ERROR... (%s) will retry in 60 seconds...' % (e,))
                time.sleep(60)
                self.update_outdated_items()
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

    def copy_harvested_properties(self, only_those = None):
        props = only_those or self.properties
        for prop in props:
            self.copy_harvested_property(prop)

    def copy_harvested_property(self, prop):
        query = 'SELECT h.wikidata_id, h.P%s, h.source FROM harvested h JOIN `%s` w ON w.wikidata_id = h.wikidata_id WHERE h.P%s IS NOT NULL AND w.P%s IS NULL' % (prop, self.name, prop, prop)
        if self.debug:
            print(query)
        self.db.cur.execute(query)
        results = self.db.cur.fetchall()
        i = 0
        t = len(results)
        print('Found %s values to write for P%s.' % (t, prop))
        if t == 0:
            return
        if not self.pywb.wikidata.logged_in():
            try:
                self.pywb.wikidata.login()
            except pywikibot.exceptions.MaxlagTimeoutError as e:
                print('ERROR... (%s) will retry in 60 seconds...' % (e,))
                time.sleep(60)
                return self.copy_harvested_property(prop)
        for (wikidata_id, title, source) in results:
            i += 1
            print('(%s/%s)' % (i, t), end=' ')
            if self.pywb.write_prop(prop, wikidata_id, title, source):
                self.mark_outdated(wikidata_id)
                self.db.cur.execute('UPDATE harvested SET P%s = NULL WHERE wikidata_id = ? AND source = ?' % (prop,), (wikidata_id, source))
            self.commit(i)
        self.commit(0)

    def copy_ciwiki_to_declaration(self):
        self.db.cur.execute('SELECT i.wikidata_id, i.title FROM interwiki i JOIN `%s` w ON w.wikidata_id = i.wikidata_id WHERE i.lang = "commonswiki" AND w.P373 IS NULL' % (self.name,))
        results = self.db.cur.fetchall()
        i = 0
        t = len(results)
        print('Found %s Commons links to write to P373.' % (t,))
        if t == 0:
            return
        if not self.pywb.wikidata.logged_in():
            try:
                self.pywb.wikidata.login()
            except pywikibot.exceptions.MaxlagTimeoutError as e:
                print('ERROR... (%s) will retry in 60 seconds...' % (e,))
                time.sleep(60)
                return self.copy_ciwiki_to_declaration()
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

    def vacuum(self):
        self.cur.execute('VACUUM')

class PYWB:
    image_properties = [18, 94, 154, 158, 1442, 1801, 3311, 3451, 5775] # jpg|jpeg|jpe|png|svg|tif|tiff|gif|xcf|pdf|djvu|webp
    integer_properties = [2971, 8366]
    item_properties = [17, 31, 131, 140, 708, 825, 1885, 5607]
    sound_properties = [51, 443, 989, 990] # ogg|oga|flac|wav|opus|mp3
    managed_properties = {
	17: { 'type': 'entity', 'constraints': [3624078, 6256], 'multiple': False },
	18: { 'type': 'image' },
	31: { 'type': 'entity', 'constraints': [], 'multiple': False },
	94: { 'type': 'image' },
	131: { 'type': 'entity', 'constraints': [515, 1549591, 56061, 15284], 'multiple': False },
	140: { 'type': 'entity', 'constraints': [879146, 13414953], 'multiple': False },
	154: { 'type': 'image' },
	158: { 'type': 'image' },
	281: { 'type': 'string' },
	373: { 'type': 'string' },
	380: { 'type': 'string' },
	443: { 'type': 'sound' },
	625: { 'type': 'coordinates' },
	708: { 'type': 'entity', 'constraints': [285181, 620225, 2072238, 2633744, 2288631, 1531518, 1778235, 1431554, 384003, 3146899, 665487, 3732788], 'multiple': False },
	825: { 'type': 'entity', 'constraints': [], 'multiple': False },
	856: { 'type': 'string' },
	1047: { 'type': 'string' },
	1435: { 'type': 'string' },
	1442: { 'type': 'image' },
	1644: { 'type': 'string' },
	1801: { 'type': 'image' },
	1866: { 'type': 'string' },
	1885: { 'type': 'entity', 'constraints': [2977], 'multiple': False },
	2971: { 'type': 'integer' },
	3311: { 'type': 'image' },
	3451: { 'type': 'image' },
	5607: { 'type': 'entity', 'constraints': [51041800, 20926517, 102496, 104145266, 17143723], 'multiple': False },
	5775: { 'type': 'image' },
	6788: { 'type': 'string' },
	8389: { 'type': 'string' },
	8366: { 'type': 'integer' },
    }
    sources = {
	'aawiki': 8558395,
	'abwiki': 3568035,
	'acewiki': 3957795,
	'adywiki': 22676953,
	'afwiki': 766705,
	'akwiki': 8558731,
	'alswiki': 1211233,
	'altwiki': 105630329,
	'amwiki': 3025527,
	'angwiki': 8558960,
	'anwiki': 1147071,
	'arcwiki': 8569951,
	'arwiki': 199700,
	'arywiki': 97393767,
	'arzwiki': 2374285,
	'astwiki': 1071918,
	'aswiki': 8559119,
	'atjwiki': 30286865,
	'avkwiki': 97393723,
	'avwiki': 5652665,
	'awawiki': 94694371,
	'aywiki': 3826575,
	'azbwiki': 20789766,
	'azwiki': 58251,
	'banwiki': 70885480,
	'barwiki': 1961887,
	'bat_smgwiki': 3568069,
	'bawiki': 58209,
	'bclwiki': 8561870,
	'be_x_oldwiki': 8937989,
	'bewiki': 877583,
	'bgwiki': 11913,
	'bhwiki': 8561277,
	'biwiki': 8561332,
	'bjnwiki': 2983979,
	'bmwiki': 8559737,
	'bnwiki': 427715,
	'bowiki': 2091593,
	'bpywiki': 1287192,
	'brwiki': 846871,
	'bswiki': 1047829,
	'bugwiki': 4097773,
	'bxrwiki': 8561415,
	'cawiki': 199693,
	'cbk_zamwiki': 8575930,
	'cdowiki': 846630,
	'cebwiki': 837615,
	'cewiki': 4783991,
	'chowiki': 8576395,
	'chrwiki': 8576237,
	'chwiki': 8576190,
	'chywiki': 8561491,
	'ckbwiki': 4115463,
	'cowiki': 3111179,
	'crhwiki': 60786,
	'crwiki': 8561582,
	'csbwiki': 3756269,
	'cswiki': 191168,
	'cuwiki': 547271,
	'cvwiki': 58215,
	'cywiki': 848525,
	'dawiki': 181163,
	'dewiki': 48183,
	'dinwiki': 32012187,
	'diqwiki': 38288,
	'dsbwiki': 8561147,
	'dtywiki': 29048035,
	'dvwiki': 928808,
	'dzwiki': 8561662,
	'eewiki': 8562097,
	'elwiki': 11918,
	'emlwiki': 3568066,
	'enwiki': 328,
	'eowiki': 190551,
	'eswiki': 8449,
	'etwiki': 200060,
	'euwiki': 207260,
	'extwiki': 3181928,
	'fawiki': 48952,
	'ffwiki': 8562927,
	'fiu_vrowiki': 1585232,
	'fiwiki': 175482,
	'fjwiki': 8562502,
	'fowiki': 8042979,
	'frpwiki': 8562529,
	'frrwiki': 8669146,
	'frwiki': 8447,
	'furwiki': 3568039,
	'fywiki': 2602203,
	'gagwiki': 79633,
	'ganwiki': 6125437,
	'gawiki': 875631,
	'gcrwiki': 74731437,
	'gdwiki': 8562272,
	'glkwiki': 3944107,
	'glwiki': 841208,
	'gnwiki': 3807895,
	'gomwiki': 20726662,
	'gorwiki': 52048523,
	'gotwiki': 8563136,
	'guwiki': 3180306,
	'gvwiki': 8566503,
	'hakwiki': 6112922,
	'hawiki': 8563393,
	'hawwiki': 3568043,
	'hewiki': 199913,
	'hifwiki': 8562481,
	'hiwiki': 722040,
	'howiki': 8563536,
	'hrwiki': 203488,
	'hsbwiki': 2402143,
	'htwiki': 1066461,
	'huwiki': 53464,
	'hywiki': 1975217,
	'hywwiki': 60437959,
	'hzwiki': 8927872,
	'iawiki': 3757068,
	'idwiki': 155214,
	'iewiki': 6167360,
	'igwiki': 8563635,
	'iiwiki': 8582909,
	'ikwiki': 8563863,
	'ilowiki': 8563685,
	'inhwiki': 47099246,
	'iowiki': 1154766,
	'iswiki': 718394,
	'itwiki': 11920,
	'iuwiki': 3913095,
	'jamwiki': 23948717,
	'jawiki': 177837,
	'jbowiki': 8566311,
	'jvwiki': 3477935,
	'kaawiki': 79636,
	'kabwiki': 8564352,
	'kawiki': 848974,
	'kbdwiki': 13231253,
	'kbpwiki': 28971705,
	'kgwiki': 8565463,
	'kiwiki': 8565476,
	'kjwiki': 8565913,
	'kkwiki': 58172,
	'klwiki': 3568042,
	'kmwiki': 3568044,
	'knwiki': 3181422,
	'koiwiki': 1116066,
	'kowiki': 17985,
	'krcwiki': 1249553,
	'krwiki': 8565254,
	'kshwiki': 3568041,
	'kswiki': 8565447,
	'kuwiki': 1154741,
	'kvwiki': 925661,
	'kwwiki': 8565801,
	'kywiki': 60799,
	'ladwiki': 3756562,
	'lawiki': 12237,
	'lbewiki': 6587084,
	'lbwiki': 950058,
	'lezwiki': 45041,
	'lfnwiki': 52047822,
	'lgwiki': 8566347,
	'lijwiki': 3568046,
	'liwiki': 2328409,
	'lldwiki': 98442371,
	'lmowiki': 3913160,
	'lnwiki': 8566298,
	'lowiki': 3568045,
	'lrcwiki': 20442276,
	'ltgwiki': 2913253,
	'ltwiki': 202472,
	'lvwiki': 728945,
	'madwiki': 104115350,
	'maiwiki': 18508969,
	'map_bmswiki': 4077512,
	'mdfwiki': 1178461,
	'mgwiki': 3123304,
	'mhrwiki': 824297,
	'mhwiki': 8568150,
	'minwiki': 4296423,
	'miwiki': 2732019,
	'mkwiki': 842341,
	'mlwiki': 874555,
	'mniwiki': 105631325,
	'mnwiki': 2998037,
	'mnwwiki': 72145810,
	'mowiki': 3568049,
	'mrjwiki': 1034940,
	'mrwiki': 3486726,
	'mswiki': 845993,
	'mtwiki': 3180091,
	'muswiki': 8569511,
	'mwlwiki': 8568791,
	'myvwiki': 856881,
	'mywiki': 4614845,
	'mznwiki': 3568048,
	'nahwiki': 2744155,
	'napwiki': 1047851,
	'nawiki': 3753095,
	'nds_nlwiki': 1574617,
	'ndswiki': 4925786,
	'newiki': 8560590,
	'newwiki': 1291627,
	'ngwiki': 8569782,
	'niawiki': 104778087,
	'nlwiki': 10000,
	'nnwiki': 2349453,
	'novwiki': 8570353,
	'nowiki': 191769,
	'nqowiki': 68669691,
	'nrmwiki': 3568051,
	'nsowiki': 13230970,
	'nvwiki': 8569757,
	'nywiki': 8561552,
	'ocwiki': 595628,
	'olowiki': 27102215,
	'omwiki': 8570425,
	'orwiki': 7102897,
	'oswiki': 226150,
	'pagwiki': 12265494,
	'pamwiki': 588620,
	'papwiki': 3568056,
	'pawiki': 1754193,
	'pcdwiki': 3568053,
	'pdcwiki': 3025736,
	'pflwiki': 13358221,
	'pihwiki': 8570048,
	'piwiki': 8570791,
	'plwiki': 1551807,
	'pmswiki': 3046353,
	'pnbwiki': 3696028,
	'pntwiki': 4372058,
	'pswiki': 3568054,
	'ptwiki': 11921,
	'quwiki': 1377618,
	'rmwiki': 3026819,
	'rmywiki': 8571143,
	'rnwiki': 8565742,
	'roa_rupwiki': 2073394,
	'roa_tarawiki': 3568062,
	'rowiki': 199864,
	'ru_sibwiki': 2996321,
	'ruewiki': 58781,
	'ruwiki': 206855,
	'rwwiki': 8565518,
	'sahwiki': 225594,
	'satwiki': 55950814,
	'sawiki': 2587255,
	'scnwiki': 1058430,
	'scowiki': 1444686,
	'scwiki': 3568059,
	'sdwiki': 8571840,
	'sewiki': 4115441,
	'sgwiki': 8571487,
	'shnwiki': 58832948,
	'shwiki': 58679,
	'simplewiki': 200183,
	'siwiki': 8571954,
	'skrwiki': 104116398,
	'skwiki': 192582,
	'slwiki': 14380,
	'smnwiki': 100154036,
	'smwiki': 8571427,
	'snwiki': 8571809,
	'sowiki': 8572132,
	'sqwiki': 208533,
	'srnwiki': 3568060,
	'srwiki': 200386,
	'sswiki': 3432470,
	'stqwiki': 3568040,
	'stwiki': 8572199,
	'suwiki': 966609,
	'svwiki': 169514,
	'swwiki': 722243,
	'szlwiki': 940309,
	'szywiki': 74732560,
	'tawiki': 844491,
	'taywiki': 105723660,
	'tcywiki': 26235862,
	'tetwiki': 8575385,
	'tewiki': 848046,
	'tgwiki': 2742472,
	'thwiki': 565074,
	'tiwiki': 8575467,
	'tkwiki': 511754,
	'tlwiki': 877685,
	'tnwiki': 3568063,
	'towiki': 3112631,
	'tpiwiki': 571001,
	'trvwiki': 105975521,
	'trwiki': 58255,
	'tswiki': 8575674,
	'ttwiki': 60819,
	'tumwiki': 8575782,
	'twwiki': 8575885,
	'tyvwiki': 14948450,
	'tywiki': 3568061,
	'udmwiki': 221444,
	'ugwiki': 60856,
	'ukwiki': 199698,
	'urwiki': 1067878,
	'uzwiki': 2081526,
	'vecwiki': 1055841,
	'vepwiki': 4107346,
	'vewiki': 8577029,
	'viwiki': 200180,
	'vlswiki': 3568038,
	'vowiki': 714826,
	'warwiki': 1648786,
	'wawiki': 1132977,
	'wmawiki': 105834398,
	'wowiki': 8582589,
	'wuuwiki': 1110233,
	'xalwiki': 4210231,
	'xhwiki': 3568065,
	'xmfwiki': 2029239,
	'yiwiki': 1968379,
	'yowiki': 1148240,
	'zawiki': 3311132,
	'zeawiki': 2111591,
	'zh_classicalwiki': 1378484,
	'zh_min_nanwiki': 3239456,
	'zh_yuewiki': 1190962,
	'zhwiki': 30239,
	'zuwiki': 8075204,
    }

    def __init__(self, user, lang):
        self.user = user
        self.site = pywikibot.Site(lang)
        self.commons = self.site.image_repository()
        self.wikidata = self.site.data_repository()
        self.items = {} # cache for Wikidata items
        self.categories = {} # cache for Commons categories
        self.pages = {} # cache for pages, per site

    def ItemPage(self, wikidata_id):
        if wikidata_id in self.items:
            return self.items[wikidata_id]
        datapage = pywikibot.ItemPage(self.wikidata, wikidata_id if format(wikidata_id).startswith('Q') else 'Q%s' % wikidata_id)
        try:
            if datapage.isRedirectPage():
                datapage = pywikibot.ItemPage(self.wikidata, datapage.getRedirectTarget().title())
        except pywikibot.exceptions.MaxlagTimeoutError as e:
            print('ERROR... (%s) will retry in 60 seconds...' % (e,))
            time.sleep(60)
            return self.ItemPage(wikidata_id)
        self.items[wikidata_id] = datapage
        return datapage

    def Category(self, title):
        if title in self.categories:
            return self.categories[title]
        category = pywikibot.Category(self.commons, 'Category:%s' % title)
        if category.isCategoryRedirect():
            category = category.getCategoryRedirectTarget()
        self.categories[title] = category
        return category

    def Claim(self, prop):
        return pywikibot.Claim(self.wikidata, prop)

    def Coordinate(self, latitude, longitude):
        return pywikibot.Coordinate(latitude, longitude, dim=10, site=self.wikidata)

    def FilePage(self, title):
        filepage = pywikibot.FilePage(self.commons, 'File:%s' % title)
        if filepage.isRedirectPage():
            filepage = self.FilePage(filepage.getRedirectTarget().title(with_ns=False))
        return filepage

    def Page(self, site_id, title):
        if site_id in self.pages and title in self.pages[site_id].keys():
            return self.pages[site_id][title]
        site = pywikibot.Site(site_id.replace('wiki', ''))
        if site_id not in self.pages:
            self.pages[site_id] = {}
        page = pywikibot.Page(site, title)
        self.pages[site_id][title] = page
        return page

    def add_claim(self, item, claim, source = None):
        if self.wikidata.logged_in() is True and self.wikidata.user() == self.user:
            try:
                if source:
                    target = None
                    qualifier = None
                    if source in self.sources:
                        target = self.ItemPage(self.sources[source])
                        qualifier = self.Claim('P143')
                    elif source.startswith('http'):
                        target = source
                        qualifier = self.Claim('P854')
                    if target and qualifier:
                        qualifier.setTarget(target)
                        claim.addSource(qualifier)
                    else:
                        print('ERROR: unknown source', source)
                item.add_claim(claim)
                print(' - added!')
            except (pywikibot.OtherPageSaveError, pywikibot.exceptions.MaxlagTimeoutError) as e:
                print('ERROR... (%s) will ignore this claim this time...' % (e,))
        else:
            print(' - error, please check you are logged in!')

    def check_constraints(self, wikidata_id, constraints):
        item = self.ItemPage(wikidata_id)
        if item.exists():
            claims = item.claims or {}
            if not constraints:
                return item
            if 'P31' in claims:
                for claim in claims['P31']:
                    nature = claim.getTarget()
                    if nature:
                        nature_id = nature.title().replace('Q', '')
                        if int(nature_id) in constraints:
                            return item
                        if nature.exists():
                            nature_claims = nature.claims or {}
                            if 'P279' in nature_claims:
                                for nature_claim in nature_claims['P279']:
                                    subclass = nature_claim.getTarget()
                                    if subclass:
                                        subclass_id = subclass.title().replace('Q', '')
                                        if int(subclass_id) in constraints:
                                            return item
        return False

    @staticmethod
    def fetch_page_templates(page):
        page['page'].templatesWithParams()

    def get_claim_value(self, prop, item):
        claims = item.claims if item.claims else {}
        pprop = 'P%s' % (prop,)
        if pprop in claims and prop in self.managed_properties:
            if self.managed_properties[prop]['type'] in ['entity', 'image', 'sound']:
                return claims[pprop][0].getTarget().title(with_ns=False) if claims[pprop][0].getTarget() else ''
            elif self.managed_properties[prop]['type'] == 'string':
                return claims[pprop][0].getTarget()
            elif self.managed_properties[prop]['type'] == 'coordinates':
                target = claims[pprop][0].getTarget()
                return '%f|%f|%f' % (float(target.lat), float(target.lon), float(target.alt if target.alt else 0)) if target else None
        return None

    def write_prop(self, prop, wikidata_id, value, source = None): # FIXME check ItemPage existence here and pass it to subfunctions
        if prop in self.item_properties:
            self.write_prop_item(prop, wikidata_id, value, source)
        elif prop in self.integer_properties:
            self.write_prop_integer(prop, wikidata_id, value, source)
        elif prop in self.image_properties:
            self.write_prop_image(prop, wikidata_id, value, source)
        elif prop == 281:
            self.write_prop_281(wikidata_id, value, source)
        elif prop == 373:
            self.write_prop_373(wikidata_id, value, source)
        elif prop == 625:
            self.write_prop_625(wikidata_id, value, source)
        elif prop == 856:
            self.write_prop_856(wikidata_id, value, source)
        elif prop == 1047:
            self.write_prop_1047(wikidata_id, value, source)
        elif prop == 1866:
            self.write_prop_1866(wikidata_id, value, source)
        elif prop == 6788:
            self.write_prop_6788(wikidata_id, value, source)
        elif prop == 8389:
            self.write_prop_8389(wikidata_id, value, source)
        else:
            print('Writing prop %s is not implemented yet! Patches are welcome!' % prop)
        if wikidata_id in self.items:
            del self.items[wikidata_id] # invalidate cache
        return True

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
                except Exception as e:
                    print(' - problem with "%s": %s' % (value, e))
                self.add_claim(item, claim, source)

    def write_descriptions(self, wikidata_id, descriptions, overwrite = False):
        item = self.ItemPage(wikidata_id)
        if item.exists():
            description_ = {}
            add_lang = []
            fix_lang = []
            for lang in descriptions.keys():
                if lang not in item.descriptions.keys():
                    description_[lang] = descriptions[lang]
                    add_lang.append(lang)
                elif overwrite and item.descriptions[lang] != descriptions[lang]:
                    description_[lang] = descriptions[lang]
                    fix_lang.append(lang)
            if len(description_.keys()) > 0:
                summaries = []
                summaries.append('Add description for ' + '/'.join(add_lang) if len(add_lang) > 0 else '')
                summaries.append('Fix description for ' + '/'.join(fix_lang) if len(fix_lang) > 0 else '')
                item.editDescriptions(description_, summary = '. '.join(summaries))

    def write_label(self, wikidata_id, lang, label, overwrite = False):
        item = self.ItemPage(wikidata_id)
        if item.exists():
            if lang not in item.labels.keys():
                item.editLabels({lang: label}, summary = 'Add %s label.' % lang)
            elif overwrite and item.labels[lang] != label:
                item.editLabels({lang: label}, summary = 'Fix %s label.' % lang)

    def write_prop_image(self, prop, wikidata_id, title, source = None):
        print('Q%s' % (wikidata_id), end='')
        title_ = title.lower()
        if not (title_.endswith(('jpg', 'jpeg')) or (prop == 94 and title_.endswith(('svg', 'png')) and 'template' not in title_ and 'coa ' not in title_ and 'coa.' not in title_)):
            print(' - Not a picture. Ignored.')
            return
        item = self.ItemPage(wikidata_id)
        if item.exists():
            pprop = 'P%s' % (prop,)
            if item.claims and pprop in item.claims:
                print(' - Image already present.')
            else:
                for prop_ in self.image_properties:
                    pprop_ = 'P%s' % (prop_,)
                    if pprop_ in item.claims:
                        for value in item.claims[pprop_]:
                            if value.getTarget().title(with_ns=False) == title:
                                print(' - Image aleady present in property %s' % pprop_)
                                return
                title = title.replace('File:', '').replace('file:', '').strip().replace('::', ':')
                if title == '':
                    print(' - no name')
                    return
                filepage = self.FilePage(title)
                print(' -', filepage.title(with_ns=False), end='')
                if filepage.exists():
                    claim = self.Claim(pprop)
                    try:
                        claim.setTarget(filepage)
                    except Exception as e:
                        print(' - wrong image "%s": %s' % (title, e))
                    self.add_claim(item, claim, source)
                else:
                    print(' - image does not exist!')

    def write_prop_integer(self, prop, wikidata_id, value, source = None):
        print('Q%s - %s' % (wikidata_id, value), end='')
        item = self.ItemPage(wikidata_id)
        if item.exists():
            pprop = 'P%s' % (prop,)
            if item.claims and pprop in item.claims:
                print(' -', pprop, 'already present.')
            else:
                try:
                    int(value)
                except:
                    print('- wrong format!')
                    return
                claim = self.Claim(pprop)
                claim.setTarget(value)
                self.add_claim(item, claim, source)

    def write_prop_281(self, wikidata_id, zip_code, source = None):
        print('Q%s - %s' % (wikidata_id, zip_code), end='')
        item = self.ItemPage(wikidata_id)
        if item.exists():
            if item.claims and 'P281' in item.claims:
                print(' - zip code already present.')
            else:
                if len(zip_code) < 2 or len(zip_code) > 20:
                    print('- wrong format!')
                    return
                claim = self.Claim('P281')
                claim.setTarget(zip_code)
                self.add_claim(item, claim, source)

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
                    claim.setTarget(commonscat.title(with_ns=False))
                    self.add_claim(item, claim, source)
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
                    latitude = float(latitude.rstrip('N'))
                    longitude = float(longitude.rstrip('E'))
                except:
                    try:
                        parts = latitude.split('/')
                        latitude = round(int(parts[0]) + int(parts[1]) / 60 + float(parts[2]) / 3600, 5)
                        if parts[3] not in ['N', 'S']:
                            raise AssertionError
                        if parts[3] == 'S':
                            latitude *= -1
                        parts = longitude.split('/')
                        longitude = round(int(parts[0]) + int(parts[1]) / 60 + float(parts[2]) / 3600, 5)
                        if parts[3] not in ['E', 'W']:
                            raise AssertionError
                        if parts[3] == 'W':
                            longitude *= -1
                    except:
                        print('- wrong format!')
                        return
                coordinates = self.Coordinate(latitude, longitude)
                claim.setTarget(coordinates)
                self.add_claim(item, claim, source)

    def write_prop_856(self, wikidata_id, website, source = None):
        print('Q%s - %s' % (wikidata_id, website), end='')
        item = self.ItemPage(wikidata_id)
        if item.exists():
            if item.claims and 'P856' in item.claims:
                print(' - website already present.')
            else:
                if not website.startswith('http'):
                    print('- wrong format!')
                    return
                claim = self.Claim('P856')
                claim.setTarget(website)
                self.add_claim(item, claim, source)

    def write_prop_1047(self, wikidata_id, catholic_hierarchy_id, source = None):
        print('Q%s - %s' % (wikidata_id, catholic_hierarchy_id), end='')
        item = self.ItemPage(wikidata_id)
        if item.exists():
            if item.claims and 'P1047' in item.claims:
                print(' - Catholic Hierarchy bishop ID already present.')
            else:
                if len(catholic_hierarchy_id) > 8:
                    print('- wrong format!')
                    return
                claim = self.Claim('P1047')
                claim.setTarget(catholic_hierarchy_id)
                self.add_claim(item, claim, source)

    def write_prop_1866(self, wikidata_id, catholic_hierarchy_id, source = None):
        print('Q%s - %s' % (wikidata_id, catholic_hierarchy_id), end='')
        item = self.ItemPage(wikidata_id)
        if item.exists():
            if item.claims and 'P1866' in item.claims:
                print(' - Catholic Hierarchy diocese ID already present.')
            else:
                if len(catholic_hierarchy_id) != 4:
                    print('- wrong format!')
                    return
                claim = self.Claim('P1866')
                claim.setTarget(catholic_hierarchy_id)
                self.add_claim(item, claim, source)

    def write_prop_6788(self, wikidata_id, messesinfo_id, source = None):
        print('Q%s - %s' % (wikidata_id, messesinfo_id), end='')
        item = self.ItemPage(wikidata_id)
        if item.exists():
            if item.claims and 'P6788' in item.claims:
                print(' - Messes.info parish ID already present.')
            else:
                if len(messesinfo_id) < 7:
                    print('- wrong format!')
                    return
                claim = self.Claim('P6788')
                claim.setTarget(messesinfo_id)
                self.add_claim(item, claim, source)

    def write_prop_8389(self, wikidata_id, gcatholic_id, source = None):
        print('Q%s - %s' % (wikidata_id, gcatholic_id), end='')
        item = self.ItemPage(wikidata_id)
        if item.exists():
            if item.claims and 'P8389' in item.claims:
                print(' - GCatholic diocese ID already present.')
            else:
                if len(gcatholic_id) > 5:
                    print('- wrong format!')
                    return
                claim = self.Claim('P8389')
                claim.setTarget(gcatholic_id)
                self.add_claim(item, claim, source)
