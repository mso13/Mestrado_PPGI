import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import re
import string
import pandas as pd

# Snorkel
from snorkel.labeling import LFAnalysis
from snorkel.labeling import PandasLFApplier
from snorkel.labeling.model import LabelModel
from snorkel.labeling import labeling_function

# General Functions

def remove_emojis(sentence):
    "Remoção de Emojis nas mensagens de texto."

    # Padrões dos Emojis
    emoji_pattern = re.compile("["
                u"\U0001F600-\U0001F64F"  # emoticons
                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                u"\U00002702-\U000027B0"
                u"\U000024C2-\U0001F251"
                u"\U0001f926-\U0001f937"
                u'\U00010000-\U0010ffff'
                u"\u200d"
                u"\u2640-\u2642"
                u"\u2600-\u2B55"
                u"\u23cf"
                u"\u23e9"
                u"\u231a"
                u"\u3030"
                u"\ufe0f"
    "]+", flags=re.UNICODE)

    return emoji_pattern.sub(r'', sentence)

def remove_valores(sentence):
    new_sentece = ''
    
    for token in sentence.split():
        if token.isdigit():
            token = '<NUM>'
        new_sentece += ' {}'.format(token)
        
    return new_sentece

def remove_links(sentence):
    new_sentece = ''
    
    for token in sentence.split():
        if token.startswith('http'):
            token = ''
        new_sentece += ' {}'.format(token)
        
    return new_sentece


class SnorkelSentimentClassifier:

    global POSITIVE, NEGATIVE, ABSTAIN, categories

    POSITIVE = 1
    NEGATIVE = 0
    ABSTAIN = -1
    categories = [POSITIVE, NEGATIVE, ABSTAIN]

    def __init__(self, df, source='twitter') -> None:
        self.df = df
        self.source = source
    
    # Labelling Functions (LFs)

    # POSITIVE
    @staticmethod
    @labeling_function()
    def lf_news_good_adjs(x):
        with open('./dicts/final/pos_adj.txt') as file:
            adjectives = [line.rstrip() for line in file]
        
        for word in x.title.lower().split():
            if word in adjectives:
                return POSITIVE
        return ABSTAIN

    @staticmethod
    @labeling_function()
    def lf_happiness_words(x):
        with open('./dicts_emocoes/alegria.txt') as f_words_happiness:
            hapiness_words = [line.rstrip() for line in f_words_happiness]
        
        for word in x.title.lower().split():
            if word in hapiness_words:
                return POSITIVE
        return ABSTAIN

    @staticmethod
    @labeling_function()
    def lf_news_good_verbs(x):
        with open('./dicts/pos_verbs.txt') as file:
            verbs = [line.rstrip() for line in file]
        
        for word in x.title.lower().split():
            if word in verbs:
                return POSITIVE
            
        return ABSTAIN

    @staticmethod
    @labeling_function()
    def lf_regex_dividendos(x):
        dividend_pattern = r".*pag.*dividendo.*|.*anunc.*dividendo.*|.*distrib.*dividendo.*"
        return POSITIVE if re.search(dividend_pattern, x.title.lower(), flags=re.I) else ABSTAIN

    @staticmethod
    @labeling_function()
    def lf_regex_resultado_positivo(x):
        raise_pattern = r"fech.*alta.*|.*abr.*alta.*|.*fech.*pos.*|.*abr.*pos.*|.*estre.*alta.*|.*prev.*alta.*|.*result.*positivo.*" 
        return POSITIVE if re.search(raise_pattern, x.title.lower(), flags=re.I) else ABSTAIN

    # NEGATIVE
    @staticmethod
    @labeling_function()
    def lf_news_bad_adjs(x):
        with open('./dicts/final/neg_adj.txt') as f_adj_neg:
            adjectives = [line.rstrip() for line in f_adj_neg]

        for word in x.title.lower().split():
            if word in adjectives:
                return NEGATIVE

        return ABSTAIN

    @staticmethod
    @labeling_function()
    def lf_sadness_words(x):
        with open('./dicts_emocoes/tristeza.txt') as f_words_sadness:
            sadness_words = [line.rstrip() for line in f_words_sadness]
        
        for word in x.title.lower().split():
            if word in sadness_words:
                return NEGATIVE

        return ABSTAIN

    @staticmethod
    @labeling_function()
    def lf_news_bad_verbs(x):
        with open('./dicts/neg_verbs.txt') as f_verb_neg:
            verbs = [line.rstrip() for line in f_verb_neg]
        
        for word in x.title.lower().split():
            if word in verbs:
                return NEGATIVE
            
        return ABSTAIN

    @staticmethod
    @labeling_function()
    def lf_regex_resultado_negativo(x):
        fall_pattern = r"fech.*queda.*|.*abr.*queda.*|.*fech.*neg.*|.*abr.*neg.*|.*prev.*baixa.*|.*prev.*queda.*|.*em.*queda.*|.*result.*negativo.*" 
        return NEGATIVE if re.search(fall_pattern, x.title.lower(), flags=re.I) else ABSTAIN    
    

    def simple_preprocessor(self, df_input: pd.DataFrame) -> pd.DataFrame:
        
        df = pd.DataFrame()

        df = df_input.copy()

        if self.source == 'twitter':
            df.rename(columns={'text': 'title'}, inplace=True)

        # Duplicate column to save original data
        df['title_raw'] = df['title']

        # Substituir símbolos importantes
        df['title'] = df['title'].map(lambda s: s.replace('-feira', ''))
        df['title'] = df['title'].map(lambda s: s.replace('-alvo', ' alvo'))
        df['title'] = df['title'].map(lambda s: s.replace('\n', ' '))  
        df['title'] = df['title'].map(lambda s: s.replace('+', ''))
        df['title'] = df['title'].map(lambda s: s.replace('º', ''))
        df['title'] = df['title'].map(lambda s: s.replace("‘", ''))
        df['title'] = df['title'].map(lambda s: s.replace("’", ''))
        df['title'] = df['title'].map(lambda s: s.replace("•", ''))
        df['title'] = df['title'].map(lambda s: s.replace('-', ''))
        df['title'] = df['title'].map(lambda s: s.replace('%', ' por cento'))
        df['title'] = df['title'].map(lambda s: s.replace('R$', ''))
        df['title'] = df['title'].map(lambda s: s.replace('U$', ''))
        df['title'] = df['title'].map(lambda s: s.replace('US$', ''))
        df['title'] = df['title'].map(lambda s: s.replace('S&P 500', 'spx'))
        df['title'] = df['title'].map(lambda s: s.replace('/', '@'))

        # Remove Links, Hashtags e Menções
        df['title'] = df['title'].map(lambda s: remove_links(s))
        df['title'] = df['title'].str.replace('(\#\w+.*?)',"")
        df['title'] = df['title'].str.replace('(\@\w+.*?)',"")

        # Transformar em String e Letras Minúsculas nas Mensagens
        df['title'] = df['title'].map(lambda s: str(s).lower())

        # Remover Pontuações
        df['title'] = df['title'].map(lambda s: s.translate(str.maketrans('', '', string.punctuation)))

        # Remover Emojis     
        df['title'] = df['title'].map(lambda s: remove_emojis(s))

        # Quebras de Linha desnecessárias
        df['title'] = df['title'].map(lambda s: s.replace('\n', ' '))

        # Remover aspas duplas
        df['title'] = df['title'].map(lambda s: s.replace('\"', ''))
        df['title'] = df['title'].map(lambda s: s.replace('“', ''))
        df['title'] = df['title'].map(lambda s: s.replace('”', ''))

        # Remover valores
        df['title'] = df['title'].map(lambda s: remove_valores(s))

        # Espaços desnecessários
        df['title'] = df['title'].map(lambda s: s.strip())

        self.df = df.copy()

        return df

    def apply_rules(self, df: pd.DataFrame):
        """
        Apply the selected LFs to textual data. Use full dataset, since there is no labelled data to compare.
        """

        # 1. Import Raw Data
        df = self.df.copy()

        # 2. Preprocess Data
        df = self.simple_preprocessor(df)

        lfs = [
            # Positive Rules
            self.lf_news_good_adjs,
            # self.lf_happiness_words,
            self.lf_news_good_verbs,
            self.lf_regex_dividendos,
            self.lf_regex_resultado_positivo,
            # Negative Rules
            self.lf_news_bad_adjs,
            # self.lf_sadness_words,
            self.lf_news_bad_verbs,
            self.lf_regex_resultado_negativo
        ]

        # apply the label model
        applier = PandasLFApplier(lfs=lfs)

        label_model = LabelModel(cardinality=len(categories),
                                device='cpu', 
                                verbose=False)

        # apply the lfs on the dataframe
        L_train = applier.apply(df=df, 
                                progress_bar=False)

        # fit on the data
        label_model.fit(L_train,
                        n_epochs=5000,
                        log_freq=100, 
                        seed=123)

        # predict and create the labels
        df['label'] = label_model.predict(L=L_train, 
                                        tie_break_policy='abstain').astype(str)

        # Convert Labels to Real classes
        dict_map = {'-1': 'NEUTRAL', '1': 'POSITIVE', '0': 'NEGATIVE'}
        df['label_class'] = df['label'].map(dict_map)

        if self.source == 'twitter':
            columns_to_keep = ['title', 'title_raw', 'created_at', 'search_dt', 'rt_count', 'favorite_count', 'label_class']
        else:
            columns_to_keep = ['title', 'title_raw', 'search_date', 'label_class']
        
        df = df[columns_to_keep]

        # Results
        results = LFAnalysis(L=L_train, lfs=lfs).lf_summary()

        return df, results
