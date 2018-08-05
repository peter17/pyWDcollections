#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import pywikibot
import requests
import sqlite3
import stat
import time

from SPARQLWrapper import SPARQLWrapper, JSON

class Collection:
    def __init__(self):
        print('Initializing...')
        self.commit_frequency = self.commit_frequency if hasattr(self, 'commit_frequency') else 50
        self.country = self.country if hasattr(self, 'country') else None
        if not (self.db and self.name and self.properties):
            print("Please define your collection's DB, name, main_type, languages and properties first.")
            return
        for prop in self.properties:
            if prop not in PYWB.managed_properties:
                print('Property %s cannot be used yet. Patches are welcome.' % (prop,))
                continue
        # FIXME adapt column type to property type
        self.db.cur.execute('CREATE TABLE IF NOT EXISTS %s (wikidata_id, %s, date_time, CONSTRAINT `unique_item` UNIQUE(wikidata_id) ON CONFLICT REPLACE)' % (self.name, ','.join(['P%s' % prop for prop in self.properties])))
        self.db.cur.execute('CREATE TABLE IF NOT EXISTS interwiki (wikidata_id, lang, title, date_time, CONSTRAINT `unique_link` UNIQUE(wikidata_id, lang) ON CONFLICT REPLACE)')
        self.db.cur.execute('CREATE TABLE IF NOT EXISTS harvested (wikidata_id, %s, source, date_time, CONSTRAINT `unique_item` UNIQUE(wikidata_id, source) ON CONFLICT REPLACE)' % (','.join(['P%s' % prop for prop in self.properties])))
        self.db.con.commit()

    def fetch(self):
        endpoint = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"
        print('Query running, please wait...')
        sparql = SPARQLWrapper(endpoint)
        keys = [self.name, 'commonslink']
        keys.extend(['P%s' % (prop,) for prop in self.properties])
        keys.extend(['label_%s' % (lang,) for lang in self.languages])
        keys.extend(['description_%s' % (lang,) for lang in self.languages])
        keys.extend(['link_%s' % (lang,) for lang in self.languages])
        keys_str = ' '.join(['?%s' % (key,) for key in keys]) + ' ?modified'
        country_filter = ('?%s wdt:P17 wd:Q%s .' % (self.name, self.country)) if self.country else ''
        condition = '{ ?%s (wdt:P31/wdt:P279*) wd:Q%s. } %s ?%s schema:dateModified ?modified ' % (self.name, self.main_type, country_filter, self.name)
        optionals = ' '.join(['OPTIONAL {?%s wdt:P%s ?P%s .}' % (self.name, prop, prop) for prop in self.properties])
        for lang in self.languages:
            optionals += ' OPTIONAL { ?%s rdfs:label ?label_%s filter (lang(?label_%s) = "%s") .}' % (self.name, lang, lang, lang)
            optionals += ' OPTIONAL { ?%s schema:description ?description_%s FILTER((LANG(?description_%s)) = "%s") . }' % (self.name, lang, lang, lang)
            optionals += ' OPTIONAL { ?link_%s schema:isPartOf [ wikibase:wikiGroup "wikipedia" ] ; schema:inLanguage "%s" ; schema:about ?%s}' % (lang, lang, self.name)
        optionals += ' OPTIONAL { ?%s ^schema:about [ schema:isPartOf <https://commons.wikimedia.org/>; schema:name ?commonslink ] . FILTER( STRSTARTS( ?commonslink, "Category:" )) . }' % (self.name,)
        langs = ','.join(self.languages)
        query = 'PREFIX schema: <http://schema.org/> SELECT DISTINCT %s WHERE { %s %s SERVICE wikibase:label { bd:serviceParam wikibase:language "%s". } }' % (keys_str, condition, optionals, langs)
        print(query)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        data = sparql.query().convert()
        if 'results' in data.keys() and 'bindings' in data['results'].keys():
            t = len(data['results']['bindings'])
            print(t, 'elements loaded')
            i = 0
            for item in data['results']['bindings']:
                i += 1
                wikidata_id = int(item[self.name]['value'].split('/')[-1].replace('Q', ''))
                print('(%s/%s) Q%s' % (i, t, wikidata_id), end='\r')
                self.db.cur.execute('INSERT OR IGNORE INTO %s (wikidata_id, date_time) VALUES (?, datetime("NOW"))' % (self.name,), (wikidata_id,))
                for prop in self.properties:
                    pprop = 'P%s' % (prop,)
                    if pprop in item.keys():
                        # FIXME convert value (URL -> wikidata_id or Wikipedia title, coord -> string, etc.)
                        self.db.cur.execute('UPDATE %s SET %s = ? WHERE wikidata_id = ?' % (self.name, pprop), (item[pprop]['value'], wikidata_id))
                for lang in self.languages:
                    if 'link_' + lang in item.keys():
                        title = item['link_' + lang]['value'].replace('https://%s.wikipedia.org/wiki/' % (lang,), '')
                        siteid = lang + 'wiki'
                        self.db.cur.execute('INSERT OR REPLACE INTO interwiki (wikidata_id, lang, title, date_time) VALUES (?, ?, ?, datetime("NOW"))', (wikidata_id, siteid, title))
                self.commit(i)
            print('')
            self.commit(0)

    def harvest_templates(self, pywb):
        for site_id in self.templates.keys():
            searched_templates = self.templates[site_id]
            props = []
            for name in searched_templates.keys():
                params = searched_templates[name]
                for param in params.keys():
                    props.append(format(params[param]))
            print('Will harvest properties', ', '.join(props), 'from', site_id)
            query = 'SELECT w.wikidata_id, i.title FROM %s w LEFT JOIN interwiki i ON w.wikidata_id = i.wikidata_id WHERE lang = "%s" AND (%s)' % (self.name, site_id, ' OR '.join(['P%s IS NULL' % prop for prop in props]))
            print(query)
            self.db.cur.execute(query)
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
                self.commit(i)
                print('(%s/%s) - %s matching templates - %s values harvested in "%s"' % (i, t, j, k, title))
            self.commit(0)

    def mark_outdated(self, wikidata_id):
        self.db.cur.execute('UPDATE %s SET date_time = NULL WHERE wikidata_id = ?' % (self.name,), (wikidata_id,))

    def update_item(self, item, pywb):
        i = 0
        wikidata_id = int(item.title().replace('Q', ''))
        for prop in self.properties:
            value = pywb.get_claim_value(prop, item)
            if value:
                i += 1
                self.db.cur.execute('UPDATE %s SET P%s = ? WHERE wikidata_id = ?' % (self.name, prop), (value, wikidata_id))
        self.db.cur.execute('UPDATE %s SET date_time = datetime("NOW") WHERE wikidata_id = ?' % (self.name,), (wikidata_id,))
        print('- %s properties updated.' % (i,))

    def update_outdated_items(self, pywb):
        self.db.cur.execute('SELECT wikidata_id FROM %s WHERE date_time IS NULL' % (self.name,))
        ids_to_update = [item[0] for item in self.db.cur.fetchall()]
        total = len(ids_to_update)
        print(total, 'elements to update.')
        i = 0
        for wikidata_id in ids_to_update:
            i += 1
            item = self.get_item(pywb, wikidata_id)
            if item.exists():
                print('(%s/%s) - Q%s' % (i, total, wikidata_id), end=' ')
                self.update_item(item, pywb)
            self.commit(i)
        self.commit(0)

    def get_item(self, pywb, wikidata_id):
        item = pywb.ItemPage(wikidata_id)
        new_id = int(item.title().replace('Q', ''))
        # If id has changed (item is a redirect), update to new one.
        if new_id != wikidata_id:
            self.db.cur.execute('UPDATE %s SET wikidata_id = ? WHERE wikidata_id = ?' % (self.name,), (new_id, wikidata_id))
        return item

    def commit(self, count):
        # Autocommit every N operations. Or now if count = 0.
        if count % self.commit_frequency == 0:
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
            self.mark_outdated(wikidata_id)
            self.commit(i)
        self.commit(0)

    def copy_harvested_images(self, pywb):
        self.db.cur.execute('SELECT wikidata_id, P18, source FROM harvested WHERE P18 IS NOT NULL AND wikidata_id IN (SELECT wikidata_id FROM %s WHERE P18 IS NULL)' % (self.name,))
        results = self.db.cur.fetchall()
        i = 0
        t = len(results)
        for (wikidata_id, title, source) in results:
            i += 1
            print('(%s/%s)' % (i, t), end=' ')
            pywb.write_prop_18(wikidata_id, title)
            self.mark_outdated(wikidata_id)
            self.db.cur.execute('UPDATE harvested SET P18 = NULL WHERE wikidata_id = ? AND source = ?', (wikidata_id, source))
            self.commit(i)
        self.commit(0)

class Database:
    def __init__(self, filepath):
        self.con = sqlite3.connect(filepath)
        self.cur = self.con.cursor()

class PYWB:
    managed_properties = [17, 18, 31, 131, 373, 380, 625, 708, 856, 1435, 1644]

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

    def get_claim_value(self, prop, item):
        claims = item.claims if item.claims else {}
        pprop = 'P%s' % (prop,)
        if pprop in claims:
            if prop in [17, 18, 31, 708, 1435, 1885]: # Item
                return claims[pprop][0].getTarget().title(withNamespace=False)
            if prop in [373, 380, 856, 1644, 1866, 2971]: # String or similar
                return claims[pprop][0].getTarget()
            if prop in [625]: # Coordinates
                target = claims[pprop][0].getTarget()
                return '%f|%f|%f' % (float(target.lat), float(target.lon), float(target.alt if target.alt else 0))
        return None

    def write_prop_18(self, wikidata_id, title):
        print('Q%s' % (wikidata_id), end='')
        if not title.lower().endswith('jpg'):
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
