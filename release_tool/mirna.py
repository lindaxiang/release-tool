import os
import sys
import click
import util
from elasticsearch import Elasticsearch
import json
import csv
import copy
import shutil
from collections import OrderedDict


def build_es(app_ctx):
    pass
    # # Do not implement the ES index for now   
    # app_es = app_ctx['config']['indexes']
    # es = Elasticsearch()

    # es.indices.create( index=index_name, ignore=400 )
    # es.indices.put_mapping(index=index_name, doc_type=index_type,
    #     body=app_ctx['es_map'])
    # donors = build_donor(app_ctx)

    # donor_fh = open(os.path.join(ctx.obj['SETTINGS']['tcga_data'], 'donor.'+ctx.obj['CURRENT_DATE']+'.jsonl'), 'w')
    # for donor_id, donor in donors.iteritems():
    #     es.index(index=index_name, doc_type=index_type, id=donor_id, body=json.loads(json.dumps(donor, default=set_default)), timeout=90)    
    #     donor_fh.write(json.dumps(donor, default=set_default) + '\n')
    # donor_fh.close()


def build_donor(app_ctx):

    schema = app_ctx['input_schemas']['donor']

    fname = os.path.join(schema['data_path'], schema['main_file_name'])

    annotations = {}
    util.read_annotations(annotations, 'pcawg_donorlist', schema['annotations_path']+'pc_annotation-pcawg_final_list.tsv')
    util.read_annotations(annotations, 'blacklist',  schema['annotations_path']+'blacklist/pc_annotation-donor_blacklist.tsv')
    util.read_annotations(annotations, 'graylist', schema['annotations_path']+'graylist/pc_annotation-donor_graylist.tsv')
    util.read_annotations(annotations, 'uuid_to_barcode', schema['annotations_path']+'pc_annotation-tcga_uuid2barcode.tsv')
    util.read_annotations(annotations, 'icgc_donor_id', schema['annotations_path']+'icgc_bioentity_ids/pc_annotation-icgc_donor_ids.csv')
    util.read_annotations(annotations, 'icgc_specimen_id', schema['annotations_path']+'icgc_bioentity_ids/pc_annotation-icgc_specimen_ids.csv')
    util.read_annotations(annotations, 'icgc_sample_id', schema['annotations_path']+'icgc_bioentity_ids/pc_annotation-icgc_sample_ids.csv')
    util.read_annotations(annotations, 'specimen_type_to_cv', schema['annotations_path']+'pc_annotation-tcga_specimen_type2icgc_cv.tsv')
    util.read_annotations(annotations, 'specimen_uuid_to_type', schema['annotations_path']+'pc_annotation-tcga_specimen_uuid2type.tsv')
    

    donor = {}
    specimen = {}
    # build the donor and specimen objects
    with open(fname, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
        for r in reader:
            donor_unique_id = r.get('disease.x')+'-US::'+r.get('participant_id')
            if not donor.get(donor_unique_id):
                #new donor: initial the donor
                donor[donor_unique_id] = create_obj(donor_unique_id, schema, r, annotations, 'donor')
            
            if not specimen.get('mirna'):
                specimen['mirna'] = {}
            specimen_id = r.get('sample_id')

            if not specimen['mirna'].get(specimen_id):
                specimen['mirna'][specimen_id] = create_obj(donor_unique_id, schema, r, annotations, 'mirna')

            aliquot = create_obj(donor_unique_id, schema, r, annotations, 'aliquot')
            specimen['mirna'][specimen_id]['aliquots'].append(copy.deepcopy(aliquot))
            specimen['mirna'][specimen_id]['donor_unique_id'] = donor_unique_id

    # build the specimen object from help file
    fname = os.path.join(schema['data_path'], schema['help_file_name'])
    with open(fname, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
        for r in reader:
            donor_unique_id = r.get('donor_unique_id')
            specimen_id = r.get('submitter_specimen_id')
            for dtype in ['rna_seq', 'wgs']:
                if not specimen.get(dtype):
                    specimen[dtype] = {}
                if not specimen[dtype].get(specimen_id):
                    specimen[dtype][specimen_id] = create_obj(donor_unique_id, schema, r, annotations, dtype)
                specimen[dtype][specimen_id]['donor_unique_id'] = donor_unique_id  

    if os.path.exists(app_ctx['output_dir']):
        shutil.rmtree(app_ctx['output_dir'])
    os.makedirs(app_ctx['output_dir'])

    # add specimen to donor
    crossref_specimen_ids = {}
    crossref_specimen_ids['rna_seq'] = specimen['rna_seq'].keys()
    crossref_specimen_ids['wgs'] = specimen['wgs'].keys()

    json2tsv_fields_map = schema['json2tsv_fields_map'] 
    sample_sheet_fields_donor = schema['target_tsv_file']['mirna_sample_sheet']['donor']
    sample_sheet_fields_specimen = schema['target_tsv_file']['mirna_sample_sheet']['specimen']
    sample_sheet_fields_sample = schema['target_tsv_file']['mirna_sample_sheet']['sample']

    with open(os.path.join(app_ctx['output_dir'], 'mirna_sample_sheet.tsv'), 'a') as f:
        f.write('\t'.join(sample_sheet_fields_donor+sample_sheet_fields_specimen+sample_sheet_fields_sample) + '\n')

    for dtype in ['mirna', 'rna_seq', 'wgs']:
        #loop on the specimen to add it to donor
        for k, v in specimen[dtype].iteritems():
            donor_unique_id = v.get('donor_unique_id')            
            if not donor.get(donor_unique_id):
                continue
            if (not donor[donor_unique_id]['pcawg_donor']) and ('tumor' in v.get('dcc_specimen_type').lower()):
                continue
            sample_sheet = OrderedDict()
            sample_sheet.update(util.get_dict_value(sample_sheet_fields_donor, donor[donor_unique_id], json2tsv_fields_map))
            # check whether wgs and rna_seq has the mirna specimen
            if dtype == 'mirna':
                v['has_matched_wgs_specimen'] = True if k in crossref_specimen_ids.get('wgs') else False
                v['has_matched_rna_seq_specimen'] = True if k in crossref_specimen_ids.get('rna_seq') else False
                
                sample_sheet.update(util.get_dict_value(sample_sheet_fields_specimen, v, json2tsv_fields_map))
                for aliquot in v.get('aliquots'):
                    sample_sheet.update(util.get_dict_value(sample_sheet_fields_sample, aliquot, json2tsv_fields_map))
                    line = util.get_line(sample_sheet)
                    # # generate mirna_sample_sheet
                    with open(os.path.join(app_ctx['output_dir'], 'mirna_sample_sheet.tsv'), 'a') as f:
                        f.write('\t'.join(line) + '\n')

            v.pop('donor_unique_id')
            if 'normal' in v.get('dcc_specimen_type').lower():
                donor[donor_unique_id]['normal'][dtype] = v
            else:
                donor[donor_unique_id]['tumor'][dtype].append(v)


    # get release_tsv fields
    release_fields = schema['target_tsv_file']['mirna_release_tsv']
    with open(os.path.join(app_ctx['output_dir'], 'mirna_release.tsv'), 'a') as f:
        f.write('\t'.join(release_fields) + '\n')
   
    # remove the donors only have tumor mirna specimen    
    for k, v in donor.iteritems():
        if (not v['pcawg_donor']) and (not v.get('normal').get('mirna')):
            continue
        for dtype in ['mirna', 'rna_seq', 'wgs']:
            v['tumor_'+dtype+'_specimen_count'] = len(v['tumor'][dtype]) if v['tumor'].get(dtype) else 0
        # generate the jsonl dump
        with open(os.path.join(app_ctx['output_dir'], 'mirna_release.jsonl'), 'a') as f:
            f.write(json.dumps(v) + '\n')

        # # generate mirna_release_tsv list
        tsv_obj = util.get_dict_value(release_fields, v, json2tsv_fields_map)
        line = util.get_line(tsv_obj)
        with open(os.path.join(app_ctx['output_dir'], 'mirna_release.tsv'), 'a') as f:
            f.write('\t'.join(line) + '\n')



def create_obj(donor_unique_id, schema, record, annotations, source):

    output_dict = copy.deepcopy(schema['target_schema'][source])

    for k, v in schema['input_fields_map'].iteritems():
        if not k in output_dict.keys(): continue
        output_dict[k] = record.get(v)
    

    if source == 'donor':
        output_dict['dcc_project_code'] = output_dict.get('dcc_project_code')+'-US'
        output_dict['donor_unique_id'] = output_dict.get('dcc_project_code')+'::'+output_dict.get('submitter_donor_id')

        # add gray-white-black label
        output_dict['pcawg_donor'] = True
        if output_dict.get('donor_unique_id') in annotations.get('blacklist'):
            output_dict['donor_wgs_exclusion_white_gray'] = 'Excluded'
        elif output_dict.get('donor_unique_id') in annotations.get('graylist'):
            output_dict['donor_wgs_exclusion_white_gray'] = 'Graylist'
        elif output_dict.get('donor_unique_id') in annotations.get('pcawg_donorlist'):
            output_dict['donor_wgs_exclusion_white_gray'] = 'Whitelist'
        else:
            output_dict['donor_wgs_exclusion_white_gray'] = 'NA'
            output_dict['pcawg_donor'] = False 

        output_dict['icgc_donor_id'] = util.get_icgc_id(donor_unique_id, \
                                                        output_dict.get('submitter_donor_id'), 'donor', annotations)
    
    if source == 'mirna':
        output_dict['icgc_specimen_id'] = util.get_icgc_id(donor_unique_id, \
                                                        output_dict.get('submitter_specimen_id'), 'specimen', annotations)
        output_dict['dcc_specimen_type'] = annotations.get('specimen_type_to_cv').get(annotations.get('specimen_uuid_to_type').\
                                                        get(output_dict.get('submitter_specimen_id')))

    if source == 'aliquot':

        output_dict['files'] = {
            'file_name': record.get('filename'),
            'file_size': record.get('files_size'),
            'file_md5sum': record.get('checksum')
        }
        output_dict['icgc_sample_id'] = util.get_icgc_id(donor_unique_id, \
                                                        output_dict.get('submitter_sample_id'), \
                                                        'sample', annotations)
        output_dict['gnos_repo'] = 'CGHub'

    if source in ['rna_seq', 'wgs']:
        for k in output_dict.keys():
            output_dict[k] = record.get(k) 

    return output_dict



    
