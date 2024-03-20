"""
Utils functions to check / fix / filter the output of dicom_conversion.py

Authors: Chris Foulon
"""
import os
import argparse

import csv

with open('/home/tolhsadum/neuro_apps/data/failed_paths.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    l = [f[0] for f in csv_reader]

# success_header = [label for label in label_dict if 'metadata' in label_dict[label] and label_dict[label]['metadata'].startswith('/media')]
log_strings = open('/home/tolhsadum/neuro_apps/data/__conversion_log_file_pdf_dataset.txt', 'r').readlines()
len(log_strings)
failed_header = [s.split('[')[-1].split(']')[0] for s in log_strings if 'is not an existing directory or a zip file' in s]
error_list = {
    'missing_root_dir': 'is not an existing directory or a zip file', # s.split('[')[-1].split(']')[0]
    'AttributeError': 'AttributeError [METADATA ERROR:', # s.split('[')[-1].split(']')[0]
    'NotImplementedError': 'NotImplementedError [METADATA ERROR:', # s.split('[')[-1].split(']')[0]
    'all_replacement': 'All the replacement fields available have been tried in [ATTRIBUTE ERROR:',#[ATTRIBUTE ERROR: input {} output ''{}]
    'save_json_fail': 'Cannot return the output full path until the file is saved', # s.split('[')[-1].split(']')[0]
    'InstanceNumber': 'InstanceNumber cannot be found in some of the dicom headers of', # InstanceNumber cannot be found in some of the dicom headers of {}. Therefore, the dicom headers cannot be sorted.
    'dcmread': 'Pydicom dcmread:', #No file
    'mismatch': 'was not added to final dict because of a mismatch' # '__dict_save in folder [{}] was not added to final dict because of a mismatch ... can be found in {}/__error_directories.txt'
}




def output_folder_integrity(output_dir):
    # __conversion_log_file.txt
    # __error_directories.txt
    # __image_label_dict.json
    if not os.path.isdir(output_dir):
        raise ValueError('[{}] is not a directory or does not exist')
    log_file = os.path.join(output_dir, '__conversion_log_file.txt')
    if not os.path.exists(log_file):
        raise ValueError('Log file not found in the output folder')
    error_dir = os.path.join(output_dir, '__error_directories.txt')
    label_dict = os.path.join(output_dir, '__image_label_dict.json')

    return


def main():
    """
    Two steps: 1) Extract the metadata
    2) conversion
    Handled errors:
    dicom_conversion.py: main():
        raise Exception('[{}] log file has not been created. Therefore, the program is stopped. Please try again'
                            ' after verifying the permission/access to the output directory.'.format(log_file_path))

        + unknown errors from the two main functions

    dicom_to_nifti.py: convert_subdir()
        logging.error('[{}] is not an existing directory or zip file'.format(root_dir))

        dicom_metadata.py: DicomSerie
            logging.warning('InstanceNumber cannot be found in some of the dicom headers of {}. '
                            'Therefore, the dicom headers cannot be sorted.'.format(self.dicom_folder))
            #failed to sort the headers because the InstanceNumber is missing

        except AttributeError as err:
            header_field = [s for s in str(err).split('\'') if s != ''][-1]
            try:
                logging.info('[{}] not found in a file from [{}], trying another one'.format(
                    header_field, dicom_dir))
        # when the attribute is not found we try to select another one to replace it until we don't have any replacement

        raise ValueError('This folder does not contain any DICOM file or there is an error')
                except ValueError:
                logging.info(
                    '[{}] from root_dir: [{}] does not contain any DICOM file or issued an error, it will'
                    ' then be skipped.'.format(dicom_dir, root_dir))
                break

        if extra_counter >= len(dicom_metadata.replacement_fields):
        logging.error('All the replacement fields available have been tried in [ATTRIBUTE ERROR: input {} output '
                      '{}] but were not found in the DICOM header'.format(dicom_dir, output_subdirectory))
        # Here, the header extraction failed, we thus can't continue the conversion TODO this need to be caught from the log

        dcm2niix_convert_folder()
                if process.stderr:
                    logging.error('STDERR in folder [{}]: [CONVERSION ERROR: {}]'.format(folder_path, process.stderr))

        try:
            tmp_series[s].save_json(output_subdirectory)
        except NotImplementedError as e:
            logging.info(
                '[{}] raised a NotImplementedError [METADATA ERROR: {}]'.format(
                    dicom_dir, e)
            )
        ==> metadata_file_field = 'failed to generate metadata'

    extra_utils.py: create_final_dict()
        if check_integrity and not check_output_integrity(dirpath):
            logging.warning('__dict_save in folder [{}] was not added to final dict because of a mismatch '
                            'between __dict_save and the content or an error during the conversion. '
                            'The list of failed conversions / metadata extraction can be found in '
                            '{}/__error_directories.txt'.format(dirpath, output_folder))
            error_list.append(dirpath)

    Of course I'm omitting all the file existence checks that are just everywhere

Where it's going to run? Power 8?
List the test cases (most representative)
List the "as long as we don't have it, it cannot run"
studynumber
    Returns
    -------

    """
    print()


if __name__ == '__main__':
    main()
