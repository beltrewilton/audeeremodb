################################################################
#   Emodb example - from audeering                             #
#   https://audeering.github.io/audformat/emodb-example.html   #
################################################################

import os
import urllib.request
import audformat
import pandas as pd

import audeer


# Get database source
source = 'http://emodb.bilderbar.info/download/download.zip'
src_dir = 'emodb-src'
if not os.path.exists(src_dir):
    urllib.request.urlretrieve(source, 'emodb.zip')
    audeer.extract_archive('emodb.zip', src_dir)


# Prepare functions for getting information from file names
def parse_names(names, from_i, to_i, is_number=False, mapping=None):
    for name in names:
        key = name[from_i:to_i]
        if is_number:
            key = int(key)
        yield mapping[key] if mapping else key


description = (
   'Berlin Database of Emotional Speech. '
   'A German database of emotional utterances '
   'spoken by actors '
   'recorded as a part of the DFG funded research project '
   'SE462/3-1 in 1997 and 1999. '
   'Recordings took place in the anechoic chamber '
   'of the Technical University Berlin, '
   'department of Technical Acoustics. '
   'It contains about 500 utterances '
   'from ten different actors '
   'expressing basic six emotions and neutral.'
)

files = sorted(
    [os.path.join('wav', f) for f in os.listdir(os.path.join(src_dir, 'wav'))]
)
names = [audeer.basename_wo_ext(f) for f in files]

emotion_mapping = {
    'W': 'anger',
    'L': 'boredom',
    'E': 'disgust',
    'A': 'fear',
    'F': 'happiness',
    'T': 'sadness',
    'N': 'neutral',
}
emotions = list(parse_names(names, from_i=5, to_i=6, mapping=emotion_mapping))

y = pd.read_csv(
    os.path.join(src_dir, 'erkennung.txt'),
    usecols=['Satz', 'erkannt'],
    index_col='Satz',
    delim_whitespace=True,
    encoding='Latin-1',
    decimal=',',
    converters={'Satz': lambda x: os.path.join('wav', x)},
).squeeze('columns')
y = y.loc[files]
y = y.replace(to_replace=u'\xa0', value='', regex=True)
y = y.replace(to_replace=',', value='.', regex=True)
confidences = y.astype('float').values

male = audformat.define.Gender.MALE
female = audformat.define.Gender.FEMALE
de = audformat.utils.map_language('de')
df_speaker = pd.DataFrame(
    index=pd.Index([3, 8, 9, 10, 11, 12, 13, 14, 15, 16], name='speaker'),
    columns=['age', 'gender', 'language'],
    data = [
        [31, male, de],
        [34, female, de],
        [21, female, de],
        [32, male, de],
        [26, male, de],
        [30, male, de],
        [32, female, de],
        [35, female, de],
        [25, male, de],
        [31, female, de],
   ],
)
speakers = list(parse_names(names, from_i=0, to_i=2, is_number=True))

transcription_mapping = {
    'a01': 'Der Lappen liegt auf dem Eisschrank.',
    'a02': 'Das will sie am Mittwoch abgeben.',
    'a04': 'Heute abend könnte ich es ihm sagen.',
    'a05': 'Das schwarze Stück Papier befindet sich da oben neben dem '
           'Holzstück.',
    'a07': 'In sieben Stunden wird es soweit sein.',
    'b01': 'Was sind denn das für Tüten, die da unter dem Tisch '
           'stehen.',
    'b02': 'Sie haben es gerade hochgetragen und jetzt gehen sie '
           'wieder runter.',
    'b03': 'An den Wochenenden bin ich jetzt immer nach Hause '
           'gefahren und habe Agnes besucht.',
    'b09': 'Ich will das eben wegbringen und dann mit Karl was '
           'trinken gehen.',
    'b10': 'Die wird auf dem Platz sein, wo wir sie immer hinlegen.',
}
transcriptions = list(parse_names(names, from_i=2, to_i=5))

db = audformat.Database(
    name='emodb',
    source=source,
    usage=audformat.define.Usage.UNRESTRICTED,
    languages=[de],
    description=description,
    meta={
        'pdf': (
            'http://citeseerx.ist.psu.edu/viewdoc/'
            'download?doi=10.1.1.130.8506&rep=rep1&type=pdf'
        ),
    },
)

# Media
db.media['microphone'] = audformat.Media(
    format='wav',
    sampling_rate=16000,
    channels=1,
)

# Raters
db.raters['gold'] = audformat.Rater()

# Schemes
db.schemes['emotion'] = audformat.Scheme(
    labels=[str(x) for x in emotion_mapping.values()],
    description='Six basic emotions and neutral.',
)
db.schemes['confidence'] = audformat.Scheme(
    'float',
    minimum=0,
    maximum=1,
    description='Confidence of emotion ratings.',
)
db.schemes['age'] = audformat.Scheme(
    'int',
    minimum=0,
    description='Age of speaker',
)
db.schemes['gender'] = audformat.Scheme(
    labels=['female', 'male'],
    description='Gender of speaker',
)
db.schemes['language'] = audformat.Scheme(
    'str',
    description='Language of speaker',
)
db.schemes['transcription'] = audformat.Scheme(
    labels=transcription_mapping,
    description='Sentence produced by actor.',
)

# MiscTable
db['speaker'] = audformat.MiscTable(df_speaker.index)
db['speaker']['age'] = audformat.Column(scheme_id='age')
db['speaker']['gender'] = audformat.Column(scheme_id='gender')
db['speaker']['language'] = audformat.Column(scheme_id='language')
db['speaker'].set(df_speaker.to_dict(orient='list'))

# MiscTable as Scheme
db.schemes['speaker'] = audformat.Scheme(
    labels='speaker',
    dtype='int',
    description=(
        'The actors could produce each sentence as often as '
        'they liked and were asked to remember a real '
        'situation from their past when they had felt this '
        'emotion.'
    ),
)

# Tables
index = audformat.filewise_index(files)
db['files'] = audformat.Table(index)

db['files']['speaker'] = audformat.Column(scheme_id='speaker')
db['files']['speaker'].set(speakers)

db['files']['transcription'] = audformat.Column(scheme_id='transcription')
db['files']['transcription'].set(transcriptions)

db['emotion'] = audformat.Table(index)
db['emotion']['emotion'] = audformat.Column(
    scheme_id='emotion',
    rater_id='gold',
)
db['emotion']['emotion'].set(emotions)
db['emotion']['emotion.confidence'] = audformat.Column(
    scheme_id='confidence',
    rater_id='gold',
)
db['emotion']['emotion.confidence'].set(confidences / 100.0)


######################################################################################################
import shutil


db_dir = audeer.mkdir('emodb')
shutil.copytree(
    os.path.join(src_dir, 'wav'),
    os.path.join(db_dir, 'wav'),
)
db.save(db_dir)

os.listdir(db_dir)





