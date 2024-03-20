# data_identification
The **data_identification** module has two purposes: 

1. Converting DICOM data into nifti while keeping all the meta-data possible in JSON files
1. Correct the meta-data when information is missing or incorrect (such as the b-value in DWI data) 

## Conversion script
**usage**: dicom_conversion.py [-h] (-p INPUT_PATH | -li- INPUT_LIST) [-o OUTPUT]
                           [-do DCM2NIIX_OPTIONS]

Convert a DICOM dataset to nifti

optional arguments:
  * -h, --help            show this help message and exit
  * -p INPUT_PATH, --input_path INPUT_PATH
                        Root folder of the dataset
  * -li- INPUT_LIST, --input_list INPUT_LIST
                        Text file containing the list of DICOM folders
  * -o OUTPUT, --output OUTPUT
                        output folder
  * -do DCM2NIIX_OPTIONS, --dcm2niix_options DCM2NIIX_OPTIONS
                        add options to the dcm2niix call between quotes (e.g. "-v y")


**Outputs**
The result of each successfully converted DICOM folders will be : 
- an "output_name".nii file
- a BIDS meta-data "output_name".json file
- "output_name"_dicom_header.json file of all the information contained in the DICOM header
- depending on the modality, one or several extra files (e.g., "output_name".bval and "output_name".bvec for DWI data).

**DWI data**
The conversion creates a 4D nifti file with all the 3D volumes on the 4th dimension. The 3D volumes will be split to be labelled individually with their b-value. 
If the b-values are unavailable, the 4D will be split, and each image's label will be 'unlabelled'