import yaml


class Config_Reader:

    def __init__(self, name):
        self.config = yaml.load(open(str(name)), Loader=yaml.SafeLoader)

    def get_listing_details(self):
        obj = Struct(self.config['listing'])
        return obj

    def get_review_details(self):
        obj = Struct(self.config['reviews'])
        return obj

    def get_logging(self):
        obj = self.config['logging']
        return obj

    def get_column_filter(self):
        obj = self.config['relation']
        return obj

    def dim_model_struct(self):
        obj = Struct(self.config['dim_model'])
        return obj

    def dim_model(self):
        obj = self.config['dim_model']
        return obj

    def get_fact_tables(self):
        obj = self.config['dim_model']['facts']
        return obj

    def get_dim_tables(self):
        obj = self.config['dim_model']['dimentions']
        return obj

    def get_fact_table_columns(self, fact):
        return self.config['dim_model'][fact+'_columns']

    def get_dim_table_columns(self, dim):
        return self.config['dim_model'][dim+'_columns']

    def get_required_columns_fact_dim(self):
        list_facts = self.get_fact_tables()
        list_dim = self.get_dim_tables()
        column_list = []
        for fact in list_facts:
            column_list.extend(self.config['dim_model'][fact+'_columns'])

        for dim in list_dim:
            column_list.extend(self.config['dim_model'][dim+'_columns'])

        return list(set(column_list))

    def get_apply_function_on_columns(self):
        obj = self.config['dim_model']['apply_function_on_columns']
        return obj

    def get_fact_save(self):
        obj = self.config['dim_model']['fact_save']
        return obj

    def get_dim_save(self):
        obj = self.config['dim_model']['dim_save']
        return obj


class Struct:
    def __init__(self, _dict):
        self.__dict__.update(_dict)
