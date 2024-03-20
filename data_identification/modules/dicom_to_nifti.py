"""
Functions to convert DICOM files to Nifti

Authors: Chris Foulon
"""
import os
import subprocess
import importlib.resources as rsc
import copy
import zipfile
import shutil
import json
import logging
from multiprocessing.dummy import Pool as ThreadPool
import multiprocessing


from data_identification.modules import dicom_metadata, extra_utils


def dcm2niix_convert_folder(folder_path, output_folder, dcm2niix_options=None):
    if not os.path.isdir(folder_path):
        raise ValueError(str(folder_path) + ' is not a directory')
    with rsc.path('data_identification.bin', 'dcm2niix') as p:
        path_to_rsc = str(p.resolve())
    if dcm2niix_options is None:
        dcm2niix_options = []

    final_opt = dcm2niix_options
    if '-f' not in dcm2niix_options:
        final_opt = ['-f', '%p_%t_%s'] + dcm2niix_options
    dcm2niix_command = [path_to_rsc, '-o', output_folder, *final_opt, folder_path]

    process = subprocess.run(dcm2niix_command,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             universal_newlines=True)
    logging.info('###STDOUT dcm2niix : {}###\n'.format(process.stdout))

    # when dcm2niix raises an error, it does it in stdout, stderr will contain something only in case of a crash
    if process.stderr:
        logging.error('STDERR in folder [{}]: [CONVERSION ERROR: {}]'.format(folder_path, process.stderr))
    return process.stdout


def convert_subdir(root_dir, output_folder, filename_format, converter_options=None, rerun='resume',
                   stop_before_pixels=True):
    """
    Convert and store the metadata of a given directory / zip archive. First, the function walks through the directory
    to list sub-folders (and uncompress every zip archive to add its folders to the list). Then, for each sub-folder
    of the list, the function will try to extract the metadata of every DICOM file and create the __dicom_metadata.json
    files. If the __dicom_metadata.json are created, the function tries to convert the DICOM data.
    Parameters
    ----------
    root_dir : str
        Path to the folder to be converted
    output_folder : str
        Absolute path to the output folder where the converted files and log files will be stored
    filename_format : str
        file format given to dcm2niix
    converter_options : List of str
        List of options given to dcm2niix ('dcm2niix -h' to see the possible options) to apply it on
        every conversion performed
    rerun : str ['resume', 'delete', 'none']
        Strategy to apply in case the output folder contains directories that has already been processed
        'delete' will just delete the directory and redo the conversion from scratch
        'resume' (default) will try to assess the integrity of the directory already present and if data is missing or
        corrupted, it will try to convert it again
        'none' (not recommended) does not handle the rerun
    stop_before_pixels : bool
        True (default) means that the header's information extracted will not contain the voxels of the dicom file

    Returns
    -------
    None
    """
    if os.path.basename(root_dir) == '':
        directory_name = os.path.basename(os.path.dirname(root_dir))
    else:
        directory_name = os.path.basename(root_dir)
    output_directory = os.path.join(output_folder, directory_name)

    if rerun == 'delete' and os.path.exists(output_directory):
        shutil.rmtree(output_directory)

    if rerun == 'resume' and os.path.exists(output_directory):
        logging.info('[{}] is already in the output directory, checking if there were errors in the '
                     'conversion'.format(output_directory))
        if all([extra_utils.check_output_integrity(d) for d, _, _ in os.walk(output_directory)]):
            logging.info(
                'No errors found in [{}], this folder will then not be processed again'.format(output_directory))
            return

    if os.path.isdir(root_dir):
        # We add all the subfolders to the list to process them one by one
        subfolder_list = [root for root, _, _ in os.walk(root_dir)]
        # We also find all the zipfile and uncompress them in case they are dicom folders
        tmp = [[os.path.join(r, ff) for ff in f if
                zipfile.is_zipfile(os.path.join(r, ff))] for r, _, f in os.walk(root_dir)]
        tmp_list = []
        for f in tmp:
            tmp_list = tmp_list + f
        for z in tmp_list:
            # So we unzipp and add the folders to the list to be processed
            logging.info('unzipping : [{}]'.format(z))
            subfolder_list = subfolder_list + extra_utils.unzip_recursive_and_list(z, output_directory)
    elif zipfile.is_zipfile(root_dir):
        # easiers here as we just unzip and add the folder tree to the folders to be processed
        logging.info('unzipping : [{}]'.format(root_dir))
        subfolder_list = extra_utils.unzip_recursive_and_list(root_dir, output_directory)
    else:
        logging.error('[{}] is not an existing directory or zip file'.format(root_dir))
        return

    tmp_filename_format = filename_format
    tmp_converter_options = converter_options
    for dicom_dir in subfolder_list:
        subdirectory_name = os.path.basename(dicom_dir)
        if len(subfolder_list) > 1:
            output_subdirectory = os.path.join(output_directory, subdirectory_name)
        else:
            # if there is only one folder, we don't need to create subfolders
            output_subdirectory = output_directory
        if os.path.exists(output_subdirectory):
            if rerun == 'resume':
                if extra_utils.check_output_integrity(output_subdirectory):
                    logging.info('[{}] was already in the output folder. As the "resume" rerun option is selected, '
                                 'this folder will be ignored'.format(output_subdirectory))
                    # if the folder contains files that correspond to the __dict_save we don't calculate it again
                    continue
        replacement_list = copy.deepcopy(dicom_metadata.replacement_fields)
        tmp_series = {}
        extra_counter = 0
        # each failed loop will delete replacement_list entries until it is empty
        while not tmp_series and replacement_list and extra_counter < len(dicom_metadata.replacement_fields):
            try:
                tmp_series = dicom_metadata.scan_dicomdir(dirpath=dicom_dir,
                                                          filename_format=tmp_filename_format,
                                                          stop_before_pixels=stop_before_pixels)
            except AttributeError as err:
                header_field = [s for s in str(err).split('\'') if s != ''][-1]
                try:
                    logging.info('[{}] not found in a file from [{}], trying another one'.format(
                        header_field, dicom_dir))
                    replacement_list = [r for r in replacement_list if r != header_field]
                    old_format_key = dicom_metadata.key_to_format_dict[header_field]
                    new_format_key = dicom_metadata.key_to_format_dict[replacement_list[0]]
                    tmp_filename_format = filename_format.replace(
                        old_format_key,
                        new_format_key)
                except KeyError as e:
                    raise e
                tmp_converter_options[converter_options.index('-f') + 1] = tmp_filename_format
                extra_counter += 1
            except ValueError:
                logging.info(
                    '[{}] from root_dir: [{}] does not contain any DICOM file or issued an error, it will'
                    ' then be skipped.'.format(dicom_dir, root_dir))
                break
        # it also means that tmp_series is empty, so the next bloc is skipped
        if extra_counter >= len(dicom_metadata.replacement_fields):
            logging.error('All the replacement fields available have been tried in [ATTRIBUTE ERROR: input {} output '
                          '{}] but were not found in the DICOM header'.format(dicom_dir, output_subdirectory))

        if tmp_series:
            """ convert the dicom folders into nifti using dcm2niix
            Note: dcm2niix doesn't handle more than 26 duplicates of the same filename and 
            will stop converting if there more files would would end up with the same name. 
            This cannot really happen now, unless one runs the scripts 26 times on the same dataset 
            without cleaning the output folder. 
            What can happen though is that we choose the wrong combination of DICOM header fields and 
            end up with non unique identifiers and so some images will be erased because considered 
            as duplicates.
            """
            # Then extra_utils.check_output_integrity(output_subdirectory) returned false.
            # So it means that dicom_dir contains dicom files but that either the conversion failed or that some
            # files are missing
            if os.path.exists(output_subdirectory):
                for f in os.listdir(output_subdirectory):
                    f_path = os.path.join(output_subdirectory, f)
                    if os.path.isfile(f_path):
                        os.remove(f_path)
            else:
                os.makedirs(output_subdirectory, exist_ok=False)
            dcm2niix_output_string = dcm2niix_convert_folder(
                folder_path=dicom_dir,
                output_folder=output_subdirectory,
                dcm2niix_options=tmp_converter_options
            )

            output_dict = extra_utils.populate_output_dict(dcm2niix_output_string)
            if output_dict:
                for pref in output_dict:
                    output_dict[pref]['input_folder'] = dicom_dir
                # we store a json file in the output_subdirectory in case the final json is not written

            else:
                # it means that tmp_serie is not empty, so we should have a metadata json file
                output_dict = {}
                for s in tmp_series:
                    output_dict[s] = {'output_dir': output_subdirectory,
                                      'input_folder': dicom_dir}

            for s in tmp_series:
                try:
                    tmp_series[s].save_json(output_subdirectory)
                    metadata_file_field = tmp_series[s].output_full_path
                except NotImplementedError as e:
                    # TODO find a fix to avoid pydicom to just break everything when the conversion fails ...
                    logging.info(
                        '[{}] raised a NotImplementedError [METADATA ERROR: {}]'.format(
                            dicom_dir, e)
                    )
                    metadata_file_field = 'failed to generate metadata'
                except AttributeError as e:
                    # TODO find a fix to avoid pydicom to just break everything when the conversion fails ...
                    logging.info(
                        '[{}] raised a AttributeError [METADATA ERROR: {}]'.format(
                            dicom_dir, e)
                    )
                    metadata_file_field = 'failed to generate metadata'
                for pref in output_dict:
                    if s in pref:
                        output_dict[pref]['metadata'] = metadata_file_field

            if '_unzip' in dicom_dir:
                for pref in output_dict:
                    output_dict[pref]['input_zip'] = root_dir
            with open(os.path.join(output_subdirectory, '__dict_save'), 'w+') as out_file:
                json.dump(output_dict, out_file, indent=4)
    # we remove all the empty folders
    extra_utils.remove_empty_folders(output_directory)
    for r, _, _ in os.walk(output_directory):
        if r.endswith('_unzip'):
            shutil.rmtree(r, ignore_errors=True)


def convert_dataset(input_path_list, output_folder, converter_options=None, rerun='resume',
                    stop_before_pixels=True, nb_cores=-1):
    """
    Format the parameters and calls the convert_subdir function in parallel to convert every zip archive and directories
    containing DICOM images.
    Parameters
    ----------
    input_path_list : List of str
        List of the absolute file paths of the folders / zip archives to be converted
    output_folder : str
        Absolute path to the output folder where the converted files and log files will be stored
    converter_options : List of str
        List of options given to dcm2niix ('dcm2niix -h' to see the possible options) to apply it on
        every conversion performed
    rerun : str ['resume', 'delete', 'none']
        Strategy to apply in case the output folder contains directories that has already been processed
        'delete' will just delete the directory and redo the conversion from scratch
        'resume' (default) will try to assess the integrity of the directory already present and if data is missing or
        corrupted, it will try to convert it again
        'none' (not recommended) does not handle the rerun
    stop_before_pixels : bool
        True (default) means that the header's information extracted will not contain the voxels of the dicom file

    Returns
    -------
    None
    """
    if converter_options is None:
        # TODO try -t option
        converter_options = []
    # dcm2niix -d 0 will force it to only convert the files at the root of a given folder so it is easier to control
    if '-d' in converter_options:
        converter_options[converter_options.index('-d') + 1] = '0'
    else:
        converter_options = ['-d', '0'] + converter_options
    if '-f' not in converter_options:
        filename_format = '%p_%t_%s'
        # converter_options = ['-f', '%k_%j_%i_%z'] + converter_options
        converter_options = ['-f', filename_format] + converter_options
    else:
        filename_format = converter_options[converter_options.index('-f') + 1]
    #     We insure that __pref__ is in every filename but avoid to have it twice, just in case.
    if '__pref__' not in filename_format:
        filename_format = filename_format + '__pref__'
        converter_options[converter_options.index('-f') + 1] = filename_format
    # we loop through all the dicom directories provided in the input-path_list
    if nb_cores == -1:
        nb_cores = multiprocessing.cpu_count()
    pool = ThreadPool(nb_cores)

    pool.map(lambda root_dir: convert_subdir(root_dir, output_folder, filename_format,
                                             converter_options=converter_options, rerun=rerun,
                                             stop_before_pixels=stop_before_pixels), input_path_list)

    pool.close()
    pool.join()

#%%
