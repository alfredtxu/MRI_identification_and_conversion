import os
import numpy as np
from shutil import copyfile
from copy import deepcopy
import logging

import nibabel as nib
from nilearn.image import iter_img


def has_bval(path):
    if not os.path.exists(path) or os.path.isdir(path):
        raise ValueError(path + ' does not exist or is a directory')
    input_name = os.path.basename(path).split('.')[0]
    input_folder = os.path.dirname(path)
    bval_path = os.path.join(input_folder, str(input_name) + '.bval')
    return os.path.exists(bval_path)


def split_4d_and_label(img_path, label_list, output_folder):
    logging.debug('splitting [{}] into {} if necessary'.format(img_path, output_folder))
    print(str(label_list))
    copy_label_list = deepcopy(label_list)
    if not os.path.isdir(output_folder):
        raise ValueError(str(output_folder) + ' does not exist or is not a directory')
    hdr = nib.load(img_path)

    paths_labels_dict = {}
    if len(hdr.shape) == 4:
        logging.debug('4d image found, splitting in into 3d images.')
        imgs = [img for img in iter_img(hdr)]
    else:
        logging.debug('only 1 dimension in the nifti, we just return the file path')
        if isinstance(label_list, list):
            if len(label_list) > 1:
                raise ValueError('The image is a 3D but there are more than one label')
            else:
                paths_labels_dict[img_path] = label_list[0]
        else:
            paths_labels_dict[img_path] = label_list

        return paths_labels_dict
    if len(imgs) != len(label_list):
        raise ValueError('4th dimension of the images must be the same as the number of labels')
    # Ensure the labels will be strings and replace the float dots by 'dot' to avoid messing up the filenames
    copy_label_list = [str(c).replace('.', 'dot') for c in copy_label_list]
    unique_dict = {label: len(copy_label_list[copy_label_list == label]) - 1 for label in copy_label_list}
    for k in unique_dict.keys():
        if unique_dict[k] == 0:
            unique_dict[k] = -1
    for i, e in reversed(list(enumerate(copy_label_list))):
        n = unique_dict[e]
        if n >= 0:
            copy_label_list[i] = '{}_{}'.format(str(e), str(n))
            unique_dict[e] -= 1
    input_name = os.path.basename(img_path).split('.')[0]
    for ind in range(len(imgs)):
        out_file_path = os.path.join(output_folder,
                                     '{}_label{}_vol{}.nii.gz'.format(str(input_name), str(copy_label_list[ind]), ind))

        nib.save(imgs[ind], out_file_path)
        logging.info('New file saved: {}'.format(out_file_path))
        paths_labels_dict[out_file_path] = copy_label_list[ind]
    return paths_labels_dict


def split_dwi4d_and_label(img_path, output_folder):
    logging.info('DWI splitting [{}] into {} is necessary'.format(img_path, output_folder))
    input_name = os.path.basename(img_path).split('.')[0]
    input_folder = os.path.dirname(img_path)
    bval_path = os.path.join(input_folder, str(input_name) + '.bval')
    if not os.path.exists(bval_path):
        print(bval_path + ' does not exist')
        print(img_path + ' has neither been split nor added to the label dictionary')
        return {}
    bvalues = np.loadtxt(bval_path)
    return split_4d_and_label(img_path, bvalues, output_folder)


def split_unlabelled(img_path, output_folder):
    if not os.path.exists(img_path):
        raise ValueError(str(img_path) + ' does not exist)')
    if not os.path.isdir(output_folder):
        raise ValueError(str(output_folder) + ' does not exist or is not a directory')
    hdr = nib.load(img_path)
    input_name = os.path.basename(img_path).split('.')[0]
    paths_labels_dict = {}
    if len(hdr.shape) == 4:
        imgs = [img for img in iter_img(hdr)]
        for ind in range(len(imgs)):
            out_file_path = os.path.join(output_folder, '{}_unlabelled_{}.nii.gz'.format(str(input_name), str(ind)))
            nib.save(imgs[ind], out_file_path)
            paths_labels_dict[out_file_path] = 'unlabelled'
    else:
        paths_labels_dict[img_path] = 'unlabelled'
        copyfile(img_path, os.path.join(output_folder, '{}_unlabelled.nii.gz'.format(str(input_name))))

    return paths_labels_dict


#%%
def split_unlabelled_dataset(path_list, output_folder):
    paths_labels_dict = {}
    for f in path_list:
        paths_labels_dict.update(split_unlabelled(img_path=f, output_folder=output_folder))
    return paths_labels_dict


def split_dwi4d_dataset(path_list, output_folder):
    paths_labels_dict = {}
    for f in path_list:
        paths_labels_dict.update(split_dwi4d_and_label(img_path=f, output_folder=output_folder))
    return paths_labels_dict
