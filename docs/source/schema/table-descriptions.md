## Table descriptions

<details>
<summary>annotation</summary>

| Name                   | Type    | Description | Example/allowed values |
| ---------------------- | ------- | ----------- | ------- |
| id                     | INTEGER | Unique identifier (Primary Key) |  |
| job_id                 | INTEGER | Annotation job unique identifier (Foreign Key) |  |
| deployment_id          | INTEGER | Deployment unique identifier (Foreign Key) |  |
| file_id                | INTEGER | Audio file unique identifier (Foreign Key) |  |
| label_id               | INTEGER | Confident sound-source, sound-type assignment (Foreign Key) |  |
| tentative_label_id     | INTEGER | Tentative sound-source, sound-type assignment (Foreign Key) |  |
| num_files              | INTEGER | Number of audio files spanned by the annotation, typically 1 |  |
| file_id_list           | JSON    | List of audio file unique identifiers (only relevant when num_files > 1) | [3, 17] |
| start_utc              | TEXT    | UTC start time in ISO8601 format (YYYY-MM-DD HH:MM:SS.SSS) |  |
| duration_ms            | INTEGER | Duration of the acoustic signal in milliseconds |  |
| start_ms               | INTEGER | Start time of acoustic signal in milliseconds relative to the beginning of the audio file |  |
| freq_min_hz            | INTEGER | Lower frequency bound of acoustic signal in Hz | 110 |
| freq_max_hz            | INTEGER | Upper frequency bound of acoustic signal in Hz  | 320 |
| channel                | INTEGER | Stereo channel number | 0,1,... |
| granularity            | TEXT    | Granularity of the annotation | call,window,batch,file,encounter |
| machine_prediction     | JSON    | Model classification scores | {"class":["KW", "HW"], "score":[0.8, 0.2]} |
| comments               | TEXT    | Additional observations |  |

</details>


<details>
<summary>job</summary>

| Name                | Type    | Description | Example/allowed values |
| ------------------- | ------- | ----------- | ------- |
| id                  | INTEGER | Unique identifier (Primary Key) |  |
| taxonomy_id         | INTEGER | Annotation taxonomy unique identifier (Foreign Key)  |  |
| annotator           | TEXT    | Name/initials of human analyst | AR |
| in_scope            | JSON    | Sound-source, sound-type combinations that were systematically annotated | ["KW;PC","HW"] |
| out_of_scope        | JSON    | Sound-source, sound-type combinations that were excluded from the systematic effort | ["%;CK"] |
| is_dense            | TEXT    | Whether the all in-scope sounds have been annotated | FALSE, TRUE (0, 1)  |
| model_id            | INTEGER | Model unique identifier (Foreign Key) |  |
| model_config        | JSON    | Model configuration settings | {"threshold":0.4} |
| start_utc           | TEXT    | UTC start date in ISO8601 format (YYYY-MM-DD) |  |
| end_utc             | TEXT    | UTC end date in ISO8601 format (YYYY-MM-DD) |  |
| by_human            | INTEGER | Whether annotations were made or validated by a human | FALSE, TRUE (0, 1)  |
| by_machine          | INTEGER | Whether annotations were made by a machine | FALSE, TRUE (0, 1)  |
| comments            | TEXT    | Additional information about the annotation job |  |

</details>


<details>
<summary>file</summary>

| Name              | Type    | Description | Example/allowed values | 
| ----------------- | ------- | ----------- | ------- |
| id                | INTEGER | Unique identifier (Primary Key) |  |
| deployment_id     | INTEGER | Deployment unique identifier (Foreign Key) |  |
| filename          | TEXT    | Audio filename | AMAR_20190406T140501.123Z.flac |
| dir_path          | TEXT    | Relative path to the directory holding the audio file | JASCO/RobertsBank/20190406 |
| sample_rate       | INTEGER | Sampling rate (no. samples per second) | 64000 |
| downsample        | TEXT    | Downsampling method, if applicable | 30 point FIR filter with Hamming window |
| num_samples       | INTEGER | Number of samples in file | 3840000 |
| format            | TEXT    | File format | FLAC |
| codec             | TEXT    | Encoding method | FLAC |
| start_utc         | TEXT    | UTC start time in ISO8601 format (YYYY-MM-DD HH:MM:SS.SSS) |  |

</details>


<details>
<summary>deployment</summary>

| Name              | Type    | Description          | Example/allowed values | 
| ----------------- | ------- | -------------------- | ------- |
| id                | INTEGER | Unique identifier (Primary Key) |  |
| name              | TEXT    | Short, descriptive name for the hydrophone deployment  | Roberts Bank | 
| owner             | TEXT    | Data owner or source | JASCO   |
| start_utc         | TEXT    | UTC start time in ISO8601 format (YYYY-MM-DD HH:MM:SS.SSS) |  |
| end_utc           | TEXT    | UTC end time in ISO8601 format (YYYY-MM-DD HH:MM:SS.SSS) |  |
| location          | TEXT    | Description of the hydrophone location | Roberts Bank, BC, Canada | 
| latitude_deg      | REAL    | Hydrophone latitude in degrees | 49.23 |
| longitude_deg     | REAL    | Hydrophone longitude in degrees | -60.1 |
| depth_m           | REAL    | Hydrophone depth in meters | 21.2 |
| trajectory        | JSON    | Position of hydrophone as a function of time (relevant for towed hydrophones) | {"time_utc":[], "latitude_deg":[], "longitude_deg":[], "depth_m":[]} 
| latitude_min_deg  | REAL    | Minimum hydrophone latitude in degrees |  | 
| latitude_max_deg  | REAL    | Maximum hydrophone latitude in degrees |  | 
| longitude_min_deg | REAL    | Minimum hydrophone longitude in degrees |  | 
| longitude_max_deg | REAL    | Maximum hydrophone longitude in degrees |  | 
| depth_min_m       | REAL    | Minimum hydrophone depth in meters |  | 
| depth_max_m       | REAL    | Maximum hydrophone depth in meters |  | 
| license           | TEXT    | License or data sharing agreement | CC BY-NC-SA |
| hydrophone        | TEXT    | Hydrophone make and model | AMAR G4 |
| bits_per_sample   | INTEGER | Number of bits per sample | 16 |
| sample_rate       | INTEGER | Sampling rate (no. samples per second) | 64000 |
| num_channels      | INTEGER | Number of channels | 1 |
| sensitivity       | REAL    | Hydrophone sensitivity | -130 dB re 1 V/μPa |
| comments          | TEXT    | Additional information about the deployment |  |

</details>


<details>
<summary>taxonomy</summary>

| Name        | Type    | Description          | Example/allowed values | 
| ----------- | ------- | -------------------- | ------- |
| id          | INTEGER | Unique identifier (Primary Key) |  |
| name        | TEXT    | Name used to identify the taxonomy | HALLO-Acoustic-Taxonomy | 
| version     | TEXT    | Version no.          | 1.0 | 
| data        | JSON    | Taxonomy tree        |  | 

</details>


<details>
<summary>model</summary>

| Name          | Type    | Description | Example/allowed values |
| ------------- | ------- | ----------- | ------- |
| id            | INTEGER | Unique identifier (Primary Key) |  |
| name          | TEXT    | Model/algorithm name | Whistle&MoanDetector |
| version       | TEXT    | Version no.          | 1.0 | 
| data          | JSON    | Metadata pertaining to the model, e.g. version no., citation, etc. |  |

</details>


<details>
<summary>label</summary>

| Name           | Type    | Description          | Example/allowed values | 
| -------------- | ------- | -------------------- | ------- |
| id             | INTEGER | Unique identifier (Primary Key) |  |
| sound_source   | TEXT    | Sound source assignment | SRKW |
| sound_type     | TEXT    | Sound type assignment | PC |

</details>


<details>
<summary>file_job_relation</summary>

| Name              | Type    | Description | Example/allowed values |
| ----------------- | ------- | ----------- | ------- |
| file_id           | INTEGER | File unique identifier (Foreign Key) |  |
| job_id            | INTEGER | Annotation job unique identifier (Foreign Key) |  |
| channel           | INTEGER | Stereo channel number | 0,1,... |

</details>
