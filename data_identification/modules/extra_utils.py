import os
import numpy as np
from glob import glob
import re
import zipfile
import csv
import json
import logging
import shutil
import copy

from multiprocessing.dummy import Pool as ThreadPool
import multiprocessing

ignored_output_dict_fields = ['output_dir', 'warning', 'info', 'input_folder', 'input_zip']


def read_bval_file(path):
    return np.loadtxt(fname=path, delimiter=' ')


def read_bvec_file(path):
    return np.loadtxt(fname=path, delimiter=' ')


def extract_pref(string):
    return string.split(os.sep)[-1]


def my_extractall(zipfile_obj, path=None, members=None, pwd=None):
    """Extract all members from the archive to the current working
       directory. `path' specifies a different directory to extract to.
       `members' is optional and must be a subset of the list returned
       by namelist(). This function return the list of the extracted
       files.
    """
    if members is None:
        members = zipfile_obj.namelist()

    output_path_list = []
    for zipinfo in members:
        output_path_list.append(zipfile_obj.extract(zipinfo, path, pwd))
    return output_path_list


def unzip_recursive_and_list(zipfile_path, output_folder, only_keep_folder_paths=True):
    if not zipfile.is_zipfile(zipfile_path):
        raise ValueError('[{}] is not a zip archive file'.format(zipfile_path))
    # we create a zipfile object
    zip_obj = zipfile.ZipFile(zipfile_path)
    # we extract everything inside the zip file to a subfolder in output folder (to avoid bugs in case the zip file
    # does not contain a directory)
    file_list = my_extractall(zip_obj, os.path.join(output_folder, os.path.basename(zipfile_path) + '_unzip'))
    # we list the zip files extracted from the original zip file if there are
    zip_list = [zz for zz in file_list if zipfile.is_zipfile(zz)]
    # while we have zip files in the output folder, we extract them in subfolders at the root of the output folder
    while zip_list:
        for z in zip_list:
            zip_obj = zipfile.ZipFile(z)
            file_list = file_list + my_extractall(zip_obj, os.path.join(
                output_folder, os.path.basename(zipfile_path) + '_unzip'))
            os.remove(z)
        zip_list = [zz for zz in file_list if zipfile.is_zipfile(zz)]
    if only_keep_folder_paths:
        file_list = [f for f in file_list if os.path.isdir(f)]
    return file_list


def create_input_path_list_from_root(root_folder_path, allow_zipfiles=True):
    if not os.path.isdir(root_folder_path):
        raise ValueError(root_folder_path + ' does not exist or is not a directory')
    input_path_list = [os.path.join(root_folder_path, p) for p in os.listdir(root_folder_path)
                       if os.path.isdir(os.path.join(root_folder_path, p)) or
                       (allow_zipfiles and zipfile.is_zipfile(os.path.join(root_folder_path, p)))]
    return input_path_list


def dcm2niix_options_filename_format(option_lst):
    try:
        index = option_lst.index('-f') + 1
    except ValueError:
        return None
    return option_lst[index]


def clean_string(string):
    forbidden_char_list = [' ', '<', '>', ':', '"', '/', '\\', '|', '?', '*']
    for c in forbidden_char_list:
        string = string.replace(c, '_')
    return string


def is_duplicate(filename, file_list):
    ll = filename.rsplit('.', 1)
    if re.match('.*[a-z]$', ll[0]):
        return '{}.{}'.format(ll[0][:-1],  ll[1]) in file_list
    else:
        return False


def clean_dcm2niix_copies(filename, folder):
    files = glob(os.path.join(folder, filename + '*'))
    # cleaned_files = [f for f in files if not is_duplicate(f, files)]
    copies = [f for f in files if is_duplicate(f, files)]
    for f in copies:
        os.remove(f)
    return copies


def clean_dcm2niix_duplicates(filename, folder):
    files = glob(os.path.join(folder, filename + '*'))
    for f in files:
        os.remove(f)
    if not os.listdir(folder):
        os.rmdir(folder)
    return files


def find_id_in_paths(id_string, paths_list):
    return [p for p in paths_list if id_string.split('_')[6] in p or id_string.split('_')[1] in p]


# import os
# import csv
# import numpy as np
# dirnames = np.loadtxt(
# '/media/chrisfoulon/DATA1/a_imagepool_mr_II/x_stroke/stroke_summary_pdf_24022020/strokePDF_Img_Yf.txt',
# dtype=str, delimiter='\n')
# folder_list = np.loadtxt('/media/chrisfoulon/DATA1/folder_list.txt', dtype=str, delimiter=' ')
# found_list = extra_utils.create_absolute_list(dirnames, folder_list)
# cleaned_list = clean_folder_lists(found_list)
# empty_list = [ind for ind, v in enumerate(found_list) if v == []]
# files_not_found = [dirnames[ind] for ind in empty_list]
# with open('found_folders.csv', 'w+') as f:
#     writer = csv.writer(f)
#     for fo in cleaned_list:
#         writer.writerow([fo])
# with open('files_not_found.csv', 'w+') as f:
#     writer = csv.writer(f)
#     for fo in files_not_found:
#         writer.writerow([fo])
def create_absolute_list(id_list_file, folder_list):
    """

    Parameters
    ----------
    id_list_file
    folder_list : str
        list of folders where the function will look for the dirnames in the dirname_list_file

    Returns
    -------
    absolute_folder_list : list
    """
    f_list = np.loadtxt(
        folder_list,
        dtype=str,
        delimiter=' ')
    id_list = np.loadtxt(
        id_list_file,
        dtype=str,
        delimiter='\n')
    subfolder_dict = {f: os.listdir(f) for f in f_list}
    subfolder_list = []
    for k in subfolder_dict.keys():
        subfolder_list = subfolder_list + [os.path.join(k, subf) for subf in subfolder_dict[k]]

    pool = ThreadPool(multiprocessing.cpu_count())

    found_list = pool.map(lambda id_string: find_id_in_paths(id_string, subfolder_list), id_list)

    pool.close()
    pool.join()
    return found_list


def get_folder_size(path):
    """

    Parameters
    ----------
    path

    Returns
    -------
    Note: Code written by Asma Khan on https://www.codespeedy.com/get-the-size-of-a-folder-in-python/
    """
    # initialize the size
    total_size = 0
    # use the walk() method to navigate through directory tree
    for dirpath, dirnames, filenames in os.walk(path):
        for i in filenames:
            # use join to concatenate all the components of path
            f = os.path.join(dirpath, i)

            # use getsize to generate size in bytes and add it to the total size
            total_size += os.path.getsize(f)
    return total_size


def clean_folder_lists(folder_list):
    found_list = []
    for f in folder_list:
        if len(f) > 1:
            folder_size = 0
            biggest = f[0]
            for ff in f:
                size = get_folder_size(ff)
                if size > folder_size:
                    folder_size = size
                    biggest = ff
            found_list.append(biggest)
        if len(f) == 1:
            found_list.append(f[0])

    return found_list


def list_full_paths_to_csv(dirnames_list_file, folder_list_file, output_folder):
    dirnames = np.loadtxt(dirnames_list_file, dtype=str, delimiter='\n')
    found_list = create_absolute_list(dirnames, folder_list_file)
    cleaned_list = clean_folder_lists(found_list)
    empty_list = [ind for ind, v in enumerate(found_list) if v == []]
    files_not_found = [dirnames[ind] for ind in empty_list]
    with open(os.path.join(output_folder, 'found_folders.csv'), 'w+') as f:
        writer = csv.writer(f)
        for fo in cleaned_list:
            writer.writerow([fo])
    with open(os.path.join(output_folder, 'files_not_found.csv'), 'w+') as f:
        writer = csv.writer(f)
        for fo in files_not_found:
            writer.writerow([fo])


def remove_empty_folders(path, remove_root=True):
    """ function to recursively remove empty folders"""
    if not os.path.isdir(path):
        return

    # remove empty subfolders
    files = os.listdir(path)
    if len(files):
        for f in files:
            full_path = os.path.join(path, f)
            if os.path.isdir(full_path):
                remove_empty_folders(full_path)

    # if folder empty, delete it
    files = os.listdir(path)
    if len(files) == 0 and remove_root:
        os.rmdir(path)


def dcm2niix_output_dict(output_prefix):
    output_dict = {}
    output_files_list = glob(output_prefix + '.*')
    for f in output_files_list:
        if f.endswith('.nii') or f.endswith('.nii.gz'):
            output_dict['output_path'] = f
        else:
            ext = os.path.basename(f).split('.', 1)[1]
            output_dict[ext] = f
    return output_dict


def parse_dcm2niix_output(string):
    lines = string.splitlines()
    begin = -1
    end = len(lines)
    """ Structure of a dcm2niix output
    Software version
    Found X DICOM file(s)
    Information and Warnings
    Convert X DICOM as [filename]
    Information and Warnings
    Convert X DICOM as [filename]
    .
    .
    .
    Conversion required [execution time]
    """
    info_warnings = {}
    converted_files = {}
    for ind, s in enumerate(lines):
        if re.match(r'^Found .* DICOM file\(s\)$', s):
            begin = ind
            info_warnings = {}
        if s.startswith('Conversion required'):
            end = ind
        if begin < ind < end:
            if not s.startswith('Convert'):
                if s.startswith('Warning'):
                    if 'warning' not in info_warnings.keys():
                        info_warnings['warning'] = []
                    info_warnings['warning'].append(s)
                else:
                    if 'info' not in info_warnings.keys():
                        info_warnings['info'] = []
                    info_warnings['info'].append(s)
            else:
                parts = s.split(' ')
                output = next(s for s in parts if os.sep in s)
                output_dir = os.path.dirname(output)
                output_pref = os.path.basename(output)
                converted_files[output_pref] = info_warnings
                converted_files[output_pref]['output_dir'] = output_dir
                info_warnings = {}
    return converted_files


def populate_output_dict(string):
    if string is None:
        return None
    d = parse_dcm2niix_output(string)
    for k in d:
        d[k].update(dcm2niix_output_dict(os.path.join(str(d[k]['output_dir']), k)))
    return d

#
# def create_dict_from_output(folder_path):
#     if not os.path.exists(folder_path):
#         raise ValueError('[{}] does not exist'.format(folder_path))
#     output_dict = {}
#     for dirpath, dirnames, filenames in os.walk(folder_path):
#         for f in filenames:
#             if '__dict_save' in os.path.join(dirpath, f):
#                 output_dict.update(json.load(open(os.path.join(dirpath, f), 'r')))
#     return output_dict


def check_missing_output_folders(output_dict):
    missing_output = []
    for k in output_dict:
        if 'output_dir' in output_dict[k]:
            if not os.path.isdir(output_dict[k]['output_dir']):
                missing_output.append(k)
    return missing_output


def check_output_integrity(output_folder):
    json_file = os.path.join(output_folder, '__dict_save')
    file_list = os.listdir(output_folder)
    # The directory only contains directories
    if all([os.path.isdir(f) for f in file_list]):
        return True
    if not os.path.exists(json_file):
        return False
    try:
        dict_save = json.load(open(json_file, 'r'))
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as err:
        logging.info('The json {} cannot be loaded properly, [JSON error: {}]. We thus run the '
                     'conversion again.'.format(json_file, err))
        return False
    for key in dict_save:
        for k in dict_save[key]:
            if k in ignored_output_dict_fields:
                continue
            else:
                if not os.path.exists(dict_save[key][k]):
                    return False
    return True


def handle_duplicate(duplicate_dict_save, duplicate_key, conflict_opt='keep_first_found'):
    if conflict_opt == 'keep_first_found':
        output_dir = duplicate_dict_save[duplicate_key]['output_dir']
        for k in duplicate_dict_save[duplicate_key]:
            if k in ignored_output_dict_fields or k == 'metadata':
                continue
            if os.path.isfile(duplicate_dict_save[duplicate_key][k]):
                logging.info('removing duplicate: ' + duplicate_dict_save[duplicate_key][k])
                os.remove(duplicate_dict_save[duplicate_key][k])
        # we check if the metadata file is not used for another file in the folder before removing it
        if 'metadata' in duplicate_dict_save[duplicate_key]:
            metadata_used_elsewhere = False
            duplicate_meta_data = duplicate_dict_save[duplicate_key]['metadata']
            if os.path.exists(duplicate_meta_data):
                for k in duplicate_dict_save:
                    if k != duplicate_key and 'metadata' in duplicate_dict_save[k] and \
                            duplicate_dict_save[k]['metadata'] == duplicate_meta_data:
                        metadata_used_elsewhere = True
            if not metadata_used_elsewhere:
                os.remove(duplicate_meta_data)
        # so only __dict_save remains
        if len(os.listdir(output_dir)) == 1:
            shutil.rmtree(output_dir, ignore_errors=True)
        else:
            del duplicate_dict_save[duplicate_key]
            if duplicate_dict_save:
                with open(os.path.join(output_dir, '__dict_save'), 'w') as out_file:
                    json.dump(duplicate_dict_save, out_file, indent=4)
            else:
                os.remove(os.path.join(output_dir, '__dict_save'))
    # if conflict_opt == 'keep_the_biggest':
    #     for k in first_found_dict:
    #         if k in duplicate_dict:


def create_final_dict(output_folder, conflict_opt='keep_first_found', check_integrity=False):
    """

    Parameters
    ----------
    output_folder : str
        Output folder of a dicom_to_nifti.convert_dataset run
    conflict_opt : str
        (default : 'keep_first_found') strategy to handle the duplicates
    check_integrity : bool
        (default : False) check (or not) if the folder has been converted properly and if not, adds this directory to
        the error file instead

    Returns
    -------
    final_dict : dict
        Keys are the identifiers returned by dcm2niix and the values are a dictionary per key listing the outputs of the
        conversion. The keys of the sub-dictionaries are among:
        'info': potential information returned by dcm2niix,
        'warning': potential warnings returned by dcm2niix,
        'output_dir': the path to the output directory,
        'output_path': the path to the converted nifti file,
        'metadata': the path to the __dicom_metadata.json file containing the header's metadata,
        'input_folder': the input folder (in case of a zip archive, the actual input will be in input_zip),
        'input_zip': in case the input folder was a zip archive, this contains its original location,
        'bval', 'bvec', 'json' ... are axtra files generated by dcm2niix depending on the modality

    error_dict : list
        List of the directory paths that failed the check_output_integrity
    """
    if not os.path.exists(output_folder):
        raise ValueError('[{}] does not exist'.format(output_folder))
    final_dict = {}
    error_list = []
    for dirpath, _, _ in os.walk(output_folder):
        json_file = os.path.join(dirpath, '__dict_save')
        if os.path.exists(json_file):
            if check_integrity and not check_output_integrity(dirpath):
                logging.warning('__dict_save in folder [{}] was not added to final dict because of a mismatch '
                                'between __dict_save and the content or an error during the conversion. '
                                'The list of failed conversions / metadata extraction can be found in '
                                '{}/__error_directories.txt'.format(dirpath, output_folder))
                error_list.append(dirpath)
            else:
                with open(json_file, 'r') as out_file:
                    dict_save = json.load(out_file)
                temp_dict = copy.deepcopy(dict_save)
                for key in temp_dict:
                    if key in final_dict:
                        # with the default option it's not very efficient but in case we add different option ...
                        handle_duplicate(dict_save, key, conflict_opt=conflict_opt)
                    else:
                        final_dict.update({key: temp_dict[key]})
    logging.info('Removing empty folders from [{}]'.format(output_folder))
    remove_empty_folders(output_folder)
    return final_dict, error_list
