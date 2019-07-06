import re
# from sample import text
import json
import pandas as pd
from math import floor
from numpy import isnan
import requests

#-----------------------------------------------------------------------------------------------------------------------------
                                                    # FOR DEBUGGING

                                    
# API_TOKEN = ''
# API_ROOT = ''

# WRITEHEADERS = {
#     'Instabase-API-Args': '{"type": "file"}',
#     'Authorization': 'Bearer {0}'.format(API_TOKEN),
# }

# def write_file(write_path, data):
#     resp =  requests.post(API_ROOT + write_path, data=data, headers=WRITEHEADERS, verify=False)

# def log_file(logname, data):

#     write_path = '/MYCC/MYCC/fs/Instabase Drive/extraction/samples/debug' + '/{}.txt'.format(logname)
#     write_file(write_path, data='{}'.format(data))


#-----------------------------------------------------------------------------------------------------------------------------

def check_validity(parsed_df):
    
    check_fields = ['employee_cont', 'employer_cont']
    
    validity = True
    
    try:
        
        for cf in check_fields:
            
            check_list = parsed_df[cf].tolist()

            for each_item in check_list:
                if float(each_item) == 0.0 and float(check_list[check_list.index(each_item)+1]) == 0.0:
                    validity = False
    
        return validity
        
    except:
        
        return validity
    
def income_cal(parsed_df, staff_app):
    
    parsed_df.drop(parsed_df[parsed_df['employee_cont'] == 0].index, inplace = True)
    parsed_df.drop(parsed_df[parsed_df['employer_cont'] == 0].index, inplace = True)
    
    if staff_app == 'true' or parsed_df['employer_cont'].sum() < parsed_df['employee_cont'].sum():

        parsed_df['bonus_employer'] = parsed_df['employer_cont'] > parsed_df['employer_cont'].min()*2

        parsed_df['employer_cont_lt_0'] = parsed_df['employer_cont'] < 0

        s_employer_cont_w_b = parsed_df[(parsed_df['bonus_employer'] == False) & (parsed_df['employer_cont_lt_0'] == False)]['employer_cont'].sum()
        c_employer_cont_w_b = len(parsed_df[(parsed_df['bonus_employer'] == False) & (parsed_df['employer_cont_lt_0'] == False)])
        
        try:
            a_employer_cont_w_b = s_employer_cont_w_b / c_employer_cont_w_b
            if isnan(a_employer_cont_w_b):
                a_employer_cont_w_b = 0
        except ZeroDivisionError:
            a_employer_cont_w_b = 0

        # s_employer_cont_b = (parsed_df[(parsed_df['bonus_employer'] == True) & (parsed_df['employer_cont_lt_0'] == False)]['employer_cont'].sum())*0.70
        s_employer_cont_b = parsed_df[(parsed_df['bonus_employer'] == True) & (parsed_df['employer_cont_lt_0'] == False)]['employer_cont'].sum()
        
        
        c_employer_cont_b = len(parsed_df[(parsed_df['bonus_employer'] == True) & (parsed_df['employer_cont_lt_0'] == False)])
        
        try:
            a_employer_cont_b = 0.70*(s_employer_cont_b - (parsed_df['employer_cont'].min()*c_employer_cont_b))/12
            # a_employer_cont_b = s_employer_cont_b / c_employer_cont_b
            if isnan(a_employer_cont_b):
                a_employer_cont_b = 0

        except ZeroDivisionError:
            a_employer_cont_b = 0

    if staff_app == 'true':
        return floor((a_employer_cont_w_b + a_employer_cont_b) / 0.17)

    parsed_df['bonus_employee'] = parsed_df['employee_cont'] > parsed_df['employee_cont'].min()*2
    parsed_df['employee_cont_lt_0'] = parsed_df['employee_cont'] < 0

    s_employee_cont_w_b = parsed_df[(parsed_df['bonus_employee'] == False) & (parsed_df['employee_cont_lt_0'] == False)]['employee_cont'].sum()
    c_employee_cont_w_b = len(parsed_df[(parsed_df['bonus_employee'] == False) & (parsed_df['employee_cont_lt_0'] == False)])
    
    try:
        a_employee_cont_w_b = s_employee_cont_w_b / c_employee_cont_w_b
        if isnan(a_employee_cont_w_b):
            a_employee_cont_w_b = 0
    except  ZeroDivisionError:
        a_employee_cont_w_b = 0
    
    
    # s_employee_cont_b = (parsed_df[(parsed_df['bonus_employee'] == True) & (parsed_df['employee_cont_lt_0'] == False)]['employee_cont'].sum())*0.70
    s_employee_cont_b = parsed_df[(parsed_df['bonus_employee'] == True) & (parsed_df['employee_cont_lt_0'] == False)]['employee_cont'].sum()
    
    
    c_employee_cont_b = len(parsed_df[(parsed_df['bonus_employee'] == True) & (parsed_df['employee_cont_lt_0'] == False)])

    try:
        a_employee_cont_b = ((s_employee_cont_b - (parsed_df['employee_cont'].min()*c_employee_cont_b))/12)*0.70
        # a_employee_cont_b = s_employee_cont_b / c_employee_cont_b
        if isnan(a_employee_cont_b):
            a_employee_cont_b = 0
    except ZeroDivisionError:
        a_employee_cont_b = 0

    if parsed_df['employer_cont'].sum() > parsed_df['employee_cont'].sum():
        return floor((a_employee_cont_w_b + a_employee_cont_b) / 0.11)

    if ((a_employee_cont_w_b + a_employee_cont_b) / 0.11) < 5000:
        return floor((a_employer_cont_w_b + a_employer_cont_b) / 0.13)
    
    return floor((a_employer_cont_w_b + a_employer_cont_b) / 0.12)
                
def income_calculate(json_data, **kwargs):
    
    try:
        
        # date_val = re.findall(r'Tarikh(.*?)\n',,re.I)[0]
        
        json_data = str(json_data).replace("'", '"').replace('u"','"')

        val_dict = json.loads(json_data)
        
        val_dict = val_dict[u'data']

        if val_dict[u'document_type'] == 'epf_hh' or 'epf_st':

            staff_app = val_dict[u'staff_application']
            
            parsed_dict = {'month' : [], 'employer_cont' : [], 'employee_cont' : []}

            for i in range(1,13,1):

                try:

                    parsed_dict[u'month'].append(val_dict[u'month_{}'.format(str(i).zfill(2))])
                    parsed_dict[u'employer_cont'].append(float(val_dict[u'employer_cont_{}'.format(str(i).zfill(2))].replace(',','')))
                    parsed_dict[u'employee_cont'].append(float(val_dict[u'employee_cont_{}'.format(str(i).zfill(2))].replace(',','')))
                
                except KeyError:

                    pass

            del val_dict
            parsed_df = pd.DataFrame.from_dict(parsed_dict)
            del parsed_dict
            
            salaried = False

            if any(i >= 3 for i in parsed_df.groupby(parsed_df['employee_cont'].tolist()).size().reset_index().rename(columns={0:'count'})['count'].tolist()):
                salaried = True

            if len(parsed_df) < 3 or (not salaried and len(parsed_df) < 6):
                return 'Required number of contributions not available'

            if not check_validity(parsed_df):
                return 'EPF document not valid'
            
            full_income = income_cal(parsed_df, staff_app)
            
            if not salaried:
                part_income = income_cal(parsed_df.tail(6), staff_app)
            else:
                part_income = income_cal(parsed_df.tail(3), staff_app)
            
            if part_income > full_income:
                return part_income
            else:
                return full_income
                
        if val_dict[u'document_type'] == 'lhdn':
            
            se = val_dict[u'staff_application']
            salaried = val_dict['lhdn_salaried']
            
            if se == 'true':
                self_employed = val_dict['lhdn_self_employed']
                business_loss = val_dict['lhdn_business_loss']
            
            final_income = floor(float(salaried))
            
            if se == 'true':
                final_income = final_income + floor(float(self_employed)) - floor(float(business_loss))

            final_income = floor((0.7 * final_income)/12)
            
            return final_income
                
        if val_dict[u'document_type'] == 'ea':
            
            ea_income = val_dict['ea_income']
            ea_com_bonus = val_dict['ea_com_bonus']
            ea_com_bonus_2 = val_dict['ea_com_bonus_2']
            
            final_income = floor((floor(float(ea_income)) + 0.7*(floor(float(ea_com_bonus)) + floor(float(ea_com_bonus_2))))/12)

            return final_income   
            
    except Exception as e:

        return 'error : ' + str(e)


#Register to Refiner Function---------------------------------------------------------------------------------------------------------

def register(name_to_fn):
    more_fns = {
        'income_calculate': {
            'fn': income_calculate,
            'ex': '',
            'desc': ''}
        }

    name_to_fn.update(more_fns)