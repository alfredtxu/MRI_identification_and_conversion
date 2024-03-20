"""
Automatic DICOM to nifti conversion trying to make up for errors

Authors: Chris Foulon
"""
import os
import sys
import argparse
import json
import logging
import csv
from datetime import datetime

import numpy as np

from data_identification.modules import dicom_to_nifti, extra_utils


""" for the -f option:
        %a=antenna (coil) name, 
        %b=basename, *
        %c=comments, *
        %d=description, *
        %e=EchoNumbers, 
        %f=folder name, *
        %i=PatientID, 
        %j=SeriesInstanceUID, 
        %k=StudyInstanceUID, 
        %m=Manufacturer, 
        %n=PatientName, 
        %p=ProtocolName, 
        %r=InstanceNumber, 
        %s=SeriesNumber, 
        %t=time, *
        %u=AcquisitionNumber, 
        %v=vendor, *
        %x=StudyID; 
        %z=SequenceName
         * don't know to which value it corresponds (or didn't calculate it) 
        """


def main():
    parser = argparse.ArgumentParser(description='Convert a DICOM dataset to nifti')
    paths_group = parser.add_mutually_exclusive_group(required=True)
    paths_group.add_argument('-p', '--input_path', type=str, help='Root folder of the dataset')
    paths_group.add_argument('-li-', '--input_list', type=str, help='Text file containing the list of DICOM folders')
    parser.add_argument('-o', '--output', type=str, help='output folder')
    parser.add_argument('-lpd', '--load_pixel_data', action='store_true',
                        help='pydicom option to load the voxel data when the metadata is read')
    parser.add_argument('-do', '--dcm2niix_options', type=str, default='',
                        help='add options to the dcm2niix call between quotes (e.g. "-v y")')

    parser.add_argument('-v', '--verbose', default='info', choices=['none', 'info', 'debug'], nargs='?', const='info',
                        type=str, help='print info or debugging messages [default is "info"] ')
    parser.add_argument('-re', '--rerun', default='resume', choices=['delete', 'resume', 'none'], nargs='?',
                        const='info', type=str,
                        help='Determine the strategy in case the program is run several times on the same'
                             'dataset. Do we delete the precedent run, do we only process images that have '
                             'not already been processed or do we do nothing?')
    parser.add_argument('-nc', '--number_of_cores', type=int, default=-1,
                        help='maximum number of cores used during the multiprocessing')
    args = parser.parse_args()
    now = datetime.now()
    log_filename = ''.join(['__conversion_log_file_', now.strftime("%m%d%Y%H%M%S"), '.txt'])
    if not os.path.exists(args.output):
        try:
            os.makedirs(args.output)
        except OSError as e:
            raise e
    else:
        if not os.path.isdir(args.output):
            raise ValueError('{} is not a directory'.format(args.output))
    log_file_path = os.path.join(args.output, log_filename)
    # error_file_path = os.path.join(os.path.dirname(os.path.dirname(args.output)), 'conversion_error_file.txt')
    output_json_file_path = os.path.join(args.output, '__image_label_dict.json')

    if args.rerun == 'delete':
        if os.path.exists(log_file_path):
            os.remove(log_file_path)

    if args.verbose == 'debug':
        logging.basicConfig(filename=log_file_path, level=logging.DEBUG)
    elif args.verbose == 'none':
        # As there is no critical message, it will actually prevent all other message to appear.
        logging.basicConfig(filename=log_file_path, level=logging.CRITICAL)
    else:
        logging.basicConfig(filename=log_file_path, level=logging.INFO)

    log_formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    file_handler = logging.StreamHandler(sys.stdout)
    file_handler.setFormatter(log_formatter)
    logging.getLogger().addHandler(file_handler)
    logging.info('log file stored in {}'.format(log_file_path))
    if not os.path.exists(log_file_path):
        raise Exception('[{}] log file has not been created. Therefore, the program is stopped. Please try again'
                        ' after verifying the permission/access to the output directory.'.format(log_file_path))

    if args.input_path is not None:
        dir_list = extra_utils.create_input_path_list_from_root(args.input_path)
    # So args.input_list is not None
    else:
        if not os.path.exists(args.input_list):
            raise ValueError(args.input_list + ' does not exist.')
        if args.input_list.endswith('.csv'):
            with open(args.input_list, 'r') as csv_file:
                dir_list = []
                for row in csv.reader(csv_file):
                    if len(row) > 1:
                        dir_list += [r for r in row]
                    else:
                        dir_list.append(row[0])
        else:
            # default delimiter is ' ', it might need to be changed
            dir_list = np.loadtxt(args.input_list, dtype=str, delimiter=' ')

    dcm2niix_options = [o for o in args.dcm2niix_options.split(' ') if o != '']
    stop_before_pixel = not args.load_pixel_data
    logging.info('Running dicom_to_nifti.convert_dataset with output in "{}", dcm2niix option "{}" and '
                 'rerun option "{}"'.format(args.output, dcm2niix_options, args.rerun))
    try:
        dicom_to_nifti.convert_dataset(dir_list, args.output, converter_options=dcm2niix_options,
                                       rerun=args.rerun, stop_before_pixels=stop_before_pixel,
                                       nb_cores=args.number_of_cores)
    except Exception as e:
        logging.exception(e)
        raise
    try:
        out_dict, error_list = extra_utils.create_final_dict(args.output, conflict_opt='keep_first_found',
                                                             check_integrity=True)
    except Exception as e:
        logging.exception(e)
        raise
    if error_list:
        with open(os.path.join(args.output, '__error_directories.txt'), 'w+') as error_file:
            json.dump(error_list, error_file)
    with open(output_json_file_path, 'w+') as out_file:
        json.dump(out_dict, out_file, indent=4)

    # print([out_dict[pref]['output_path'] for pref in out_dict])
    # for p in out_dict:
    #     if 'bval'in out_dict[p]:
    #         print(out_dict[p]['bval'])
    #
    # to_be_labelled_list = [out_dict[pref]['output_path'] for pref in out_dict
    #                        if 'bval' in out_dict[pref]]
    # unlabelled_list = [out_dict[pref]['output_path'] for pref in out_dict]
    # print('to_be_labelled_list')
    # print(to_be_labelled_list)
    # split_output = os.path.join(args.output, '__split_nifti')
    # unlabelled_split_output = os.path.join(args.output, '__unlabelled_split_nifti')
    # os.makedirs(split_output, exist_ok=True)
    # os.makedirs(unlabelled_split_output, exist_ok=True)
    # paths_labels_dict = nifti_utils.split_dwi4d_dataset(to_be_labelled_list, split_output)
    # paths_unlabelled_dict = nifti_utils.split_unlabelled_dataset(unlabelled_list, unlabelled_split_output)
    # # final_paths_labels_dict = paths_labels_dict.copy().update(paths_unlabelled_dict)
    # final_paths_labels_dict = paths_labels_dict.copy()
    # # labels = list(paths_labels_dict.values())
    # with open(os.path.join(os.path.dirname(os.path.dirname(args.output)), '__final_dict.json'), 'w+') as out_file:
    #     json.dump(final_paths_labels_dict, out_file, indent=4)


if __name__ == '__main__':
    main()
