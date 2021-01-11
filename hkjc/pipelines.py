from scrapy.exporters import CsvItemExporter
from scrapy import signals
from datetime import datetime
import os


def item_type(item):
    return type(item).__name__.replace('Item','').lower()  # TeamItem => team

class MultiCSVItemPipeline(object):
    SaveTypes = ['race','dividend','sectional', 'horseinfo', 'horseform'] #lowercase
    CSVDir = "./" #directory name
    current_dt = datetime.now().strftime("%Y%m%d%H%M")

    def __init__(self):
        self.files = {}
        self.filenames = {}
        self.exporters = {}
    
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        self.filenames = dict([ (name, self.CSVDir+name+'_'+self.current_dt+'.csv') for name in self.SaveTypes])
        self.files = dict([ (name, open(self.filenames[name],'w+b')) for name in self.SaveTypes ])
        self.exporters = dict([ (name,CsvItemExporter(self.files[name])) for name in self.SaveTypes])
        # [e.start_exporting() for e in self.exporters.values()]
        for name in self.SaveTypes:
            if name == 'race':
                self.exporters[name].fields_to_export = ['race_id', 'date', 'venue', 'race_no', 'name', 'race_type',
                            'prize', 'going', 'course', 'placing', 'horse_no',
                            'horse', 'horse_url','jockey', 'trainer', 'actual_wt', 'declared_wt',
                            'draw', 'lbw', 'running_position', 'finish_time', 'win_odds']

            if name == 'dividend':
                self.exporters[name].fields_to_export = ['race_id', 'date', 'venue', 'race_no', 'pool', 'winning_combo',
                                'dividend']

            if name == 'sectional':
                self.exporters[name].fields_to_export = ['date', 'venue', 'race_no', 'race_time', 'sectional_time', 
                            'finishing_order', 'horse_no', 'horse', 
                            'sec1_position', 'sec1_lbw', 'sec1_time', 'sec2_position', 'sec2_lbw', 'sec2_time', 
                            'sec3_position', 'sec3_lbw', 'sec3_time', 'sec4_position', 'sec4_lbw', 'sec4_time', 
                            'sec5_position', 'sec5_lbw', 'sec5_time', 'sec6_position', 'sec6_lbw', 'sec6_time', 
                            'finish_time',]
            
            if name == 'horseinfo':
                self.exporters[name].fields_to_export = ['horse_id', 'horse', 'retired', 'rating_url', 'distance_url', 
                            'trackwork_url', 'veterinary_url', 'movement_url', 'overseas_url', 'pedigree_url', 
                            'country_origin', 'colour_sex', 'import_type', 'total_stakes', 'total_123_starts', 
                            'current_stable', 'trainer', 'owner', 'last_rating', 'sire', 'dam', 'dams_sire', 'same_sire']

            if name == 'horseform':
                self.exporters[name].fields_to_export = ['horse_id', 'horse', 'season', 'race_id', 'race_url', 'placing', 
                            'date', 'course', 'distance', 'going', 'race_class', 'draw', 'rating', 'trainer', 'jockey', 
                            'lbw', 'win_odds', 'actual_wt', 'running_position', 'finish_time', 'declared_wt', 'gear']

            self.exporters[name].start_exporting()

    def spider_closed(self, spider):
        [e.finish_exporting() for e in self.exporters.values()]
        [f.close() for f in self.files.values()]
        [os.remove(csv) for csv in self.filenames.values() if os.stat(csv).st_size == 0]

    def process_item(self, item, spider):
        what = item_type(item)
        if what in set(self.SaveTypes):
            self.exporters[what].export_item(item)
        return item