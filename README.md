A Python framework based on pyWikibot to harvest Wikipedia templates to Wikidata.

# Usage example

This harvests properties Commonscat (P373), image (P18) and administrative location (P131) from the English Wikipedia templates "Commonscat" and "Infobox cemetery".

Running this will create a local SQLite database, download all cemeteries items from Wikidata and for the items that miss at least one of those three properties, it will scan all English Wikipedia articles related to the items to search for those properties in those templates.

    #!/usr/bin/env python3
    # -*- coding: utf-8 -*-

    import os
    import pywdcollections as PYWDC

    path = os.path.dirname(os.path.realpath(__file__))

    class Cemeteries(PYWDC.Collection):
        def __init__(self, pywb):
            self.db = PYWDC.Database(path + '/cemeteries.db')
            self.name = 'cemeteries'
            self.commit_frequency = 10000
            self.main_type = 39614 # cemetery
            self.properties = [18, 131, 373]
            self.languages = ['en']
            self.templates = {
                'enwiki': {
                    'Commonscat': 373,
                    'Infobox cemetery': {
                        'image': 18,
                        'location': 131,
                    },
                },
        }
        super().__init__(pywb)

    if __name__ == '__main__':
        pywb = PYWDC.PYWB('<YOUR_BOT_NAME>', 'en')
        pywb.wikidata.login()
        collection = Cemeteries(pywb)
        collection.fetch()
        collection.copy_ciwiki_to_declaration()
        collection.update_outdated_items()
        collection.harvest_templates()
        collection.copy_harvested_properties([18, 131, 373])
