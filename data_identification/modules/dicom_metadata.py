import os
import json
import logging
import re

import pydicom
from pydicom.sequence import Sequence
from data_identification.modules import extra_utils

# based on https://github.com/pydicom/contrib-pydicom/blob/master/input-output/pydicom_series.py

"""
1) list all the dicom files of a directory
2) Group them by their SeriesInstanceUID
3) Create a Volume object for each SeriesInstanceUID and try to merge the headers (for the fields that are different,
store the values into a list
Maybe add a mechanism to anonymize the headers by filtering out some fields.  
"""

"""
'%a': antenna(coil)name,
'%b': basename, *
'%c': comments, *
'%d': description, *
'%t': time, (StudyDate + round(StudyTime))
'%f': foldername, *
    '%t': 'StudyTime',
'%p_%t_%s' is the default
"""
format_to_key_dict = {
        '%d': 'SeriesDescription',
        '%i': 'PatientID',
        '%j': 'SeriesInstanceUID',
        '%k': 'StudyInstanceUID',
        '%m': 'Manufacturer',
        '%n': 'PatientName',
        '%p': 'ProtocolName',
        '%r': 'InstanceNumber',
        '%s': 'SeriesNumber',
        '%u': 'AcquisitionNumber',
        '%x': 'StudyID',
        '%z': 'SequenceName'
    }
# keys become values(dcm2niix string format). Works only because all the values are unique
key_to_format_dict = {v: k for k, v in format_to_key_dict.items()}

replacement_fields = [
    'SequenceName',
    'StudyInstanceUID',
    'StudyID'
]


def string_format_to_header_keys(string):
    """

    Parameters
    ----------
    string

    Returns
    -------

    """
    return [format_to_key_dict[a] for a in string.split('_') if a != '']


def extract_identifier_list_from_string(string):
    identifier_index_list = [i.span() for i in re.finditer(r'%[a-z]', string)]
    identifier_list = [string[i[0]:i[1]] for i in identifier_index_list]
    return identifier_list


def create_metadata_filename(identifier_string, dcm, dicom_folder=''):
    """

    Parameters
    ----------
    identifier_string
    dcm
    dicom_folder

    Returns
    -------

    Notes
    -----
    '%b': basename and '%c': comments are not yet handled
    """

    identifier_list = re.findall(r'%[a-z]', identifier_string)
    filename = identifier_string
    for identifier in identifier_list:
        if identifier in format_to_key_dict:
            cleaned_identifier = str(getattr(dcm, format_to_key_dict[identifier]))
        elif identifier == '%t':
            cleaned_identifier = '{}{}'.format(dcm.StudyDate, str(round(float(dcm.StudyTime))))
            # cleaned_identifier = '{}{}'.format(dcm.StudyDate, dcm.StudyTime.split('.')[0])
        elif identifier == '%a':
            raise ValueError('%a antenna (coil) name is not present in every header and thus'
                             ' cannot be used in filename')
        elif identifier == '%f':
            if dicom_folder != '' and dicom_folder is not None:
                cleaned_identifier = dicom_folder
            else:
                raise ValueError('dicom_folder must be defined to be able to use the %f option')
        else:
            raise ValueError('Error, {} identifier is not handled'.format(identifier))
        filename = filename.replace(identifier, cleaned_identifier)
    return extra_utils.clean_string(filename)


class DicomSerie(object):

    def __init__(self, dcm, identifier_string, dicom_dir):
        """
        
        """
        self.identifier_string = identifier_string
        self.dicom_folder = dicom_dir
        self._datasets = Sequence()
        self._datasets.append(dcm)
        self._generated_prefix = create_metadata_filename(identifier_string, dcm=dcm, dicom_folder=self.dicom_folder)
        self._output_filename = self.generated_prefix + '_dicom_metadata.json'
        logging.info('New dicom serie created with filename: {}'.format(self.output_filename))
        self.metadata_json_dict = {}
        self._output_full_path = None

    def append(self, dcm):
        """ append(dcm)
        Append a dicomfile (as a pydicom.dataset.FileDataset) to the series.
        """
        temp_out_prefix = create_metadata_filename(self.identifier_string, dcm=dcm, dicom_folder=self.dicom_folder)
        if temp_out_prefix == self.generated_prefix:
            self._datasets.append(dcm)
            return True
        else:
            return False

    def _sort(self):
        """ _sort()
        Sort the datasets by instance number.
        """
        self._datasets.sort(key=lambda k: k.InstanceNumber)

    @property
    def dicom_folder(self):
        return self._dicom_folder

    @dicom_folder.setter
    def dicom_folder(self, dicom_dir):
        if os.path.isdir(dicom_dir):
            self._dicom_folder = dicom_dir
        else:
            raise ValueError('{} does not exist or is not a directory'.format(dicom_dir))

    @property
    def generated_prefix(self):
        return self._generated_prefix

    @property
    def identifier_string(self):
        return self._identifier_string

    @identifier_string.setter
    def identifier_string(self, identifier_string):
        if identifier_string is None or identifier_string == '':
            raise ValueError('The dcm2niix format string must be defined to identify the series')
        self._identifier_string = identifier_string

    @property
    def metadata_json_dict(self):
        return self._metadata_json_dict

    @metadata_json_dict.setter
    def metadata_json_dict(self, value):
        self._metadata_json_dict = value

    @property
    def output_filename(self):
        return self._output_filename

    @property
    def output_full_path(self):
        if self._output_full_path is not None:
            return self._output_full_path
        else:
            logging.warning(
                'Warning in [{}] : Cannot return the output full path until the file is saved'.format(
                    self.output_filename))
            return ''

    @output_full_path.setter
    def output_full_path(self, value):
        self._output_full_path = value

    def generate_metadata(self):
        try:
            self._sort()
        except TypeError as e:
            logging.warning('InstanceNumber cannot be found in some of the dicom headers of {}. '
                            'Therefore, the dicom headers cannot be sorted.'.format(self.dicom_folder))
        # As the object is initialized with a Dataset, the length cannot be lower than 1
        if len(self._datasets) == 1:
            self.metadata_json_dict = self._datasets[0].to_json_dict()
            return self.metadata_json_dict

        json_list = [d.to_json_dict() for d in self._datasets]
        keys_set = set()
        for json_dict in json_list:
            keys_set.update(json_dict.keys())

        metadata_list_dict = {key: [d[key] if key in d.keys() else None for d in json_list]
                              for key in keys_set}

        for key in metadata_list_dict:
            if metadata_list_dict[key].count(metadata_list_dict[key][0]) == len(metadata_list_dict[key]):
                self.metadata_json_dict[key] = metadata_list_dict[key][0]
            else:
                self.metadata_json_dict[key] = metadata_list_dict[key]

        return self.metadata_json_dict

    def save_json(self, output_dir, output_filename=''):
        if not self.metadata_json_dict:
            self.generate_metadata()
        if output_filename != '':
            out = output_filename
        else:
            out = self.output_filename
        self.output_full_path = os.path.join(output_dir, out)
        with open(self.output_full_path, 'w+') as json_fd:
            json.dump(self.metadata_json_dict, json_fd)


def scan_dicomdir(dirpath, filename_format='%t_%s', stop_before_pixels=True):
    """

    Parameters
    ----------
    filename_format
    dirpath : str
        existing DICOM directory to be scanned
    stop_before_pixels : bool
        True means that the actual voxel values won't be read and won't be added to the meta-data

    Returns
    -------
    series : dict
        dictionary of the DicomSeries created from the DICOM directory with their StudyInstanceUID_SeriesInstanceUID
        as key.
    """
    logging.info('Extracting metadata from : {}'.format(dirpath))
    series = {}
    # identifier_list = filename_format.split('_')
    file_list = [os.path.join(dirpath, f) for f in os.listdir(dirpath) if not os.path.isdir(os.path.join(dirpath, f))]

    for filepath in file_list:
        # Try loading dicom
        try:
            dcm = pydicom.dcmread(filepath, defer_size=None, stop_before_pixels=stop_before_pixels, force=False)
        except pydicom.filereader.InvalidDicomError:
            continue  # skip non-dicom file
        except Exception as why:
            logging.error('Pydicom dcmread:', why)
            break

        # Get identifiers and register the file with an existing or new series object
        # for i in identifier_list:
        #     if i not in dcm:
        #         raise ValueError('{} is not in the header of the DICOM image'.format(i))

        # logging.debug('identifier list : ' + str(identifier_list))

        # logging.info('metadata output_filename : ' + output_filename)

        dicom_serie_id = create_metadata_filename(identifier_string=filename_format, dcm=dcm, dicom_folder=dirpath)
        if dicom_serie_id not in series:
            series[dicom_serie_id] = DicomSerie(dcm=dcm, identifier_string=filename_format, dicom_dir=dirpath)
        else:
            series[dicom_serie_id].append(dcm)
    if len(file_list) == 0 or not series:
        raise ValueError('This folder does not contain any DICOM file or there is an error')

    # for s in series:
    #     series[s].save_json(output_dir)

    return series


def save_dicom_metadata():
    return
