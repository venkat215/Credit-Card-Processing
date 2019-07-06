import re
import json
import requests
import os
import tempfile

# NP1                                    
API_TOKEN = ''
API_ROOT = ''

#NP2
# API_TOKEN = ''
# API_ROOT = ''

READHEADERS = {
    'Instabase-API-Args': '{"type": "file", "get_content": true}',
    'Authorization': 'Bearer {0}'.format(API_TOKEN),
}

def read_fileid(read_path):
    return requests.get(
    API_ROOT + read_path, headers=READHEADERS, verify=False
    )

#income extraction function
def income_extract(doc_type, inputstring, **kwargs):
    
    try:
        
        #read input parameters
        r = read_fileid('/MYCC/MYCC/fs/Instabase Drive/extraction/samples/out/extraction/input/input.json')
        # r = read_fileid('/MYCC/MYCC/fs/Instabase Drive/extraction/samples/input/input.json')
        with open(tempfile.gettempdir() + '/input.json', 'wb') as f:
            for chunk in r.iter_content(chunk_size=128):
                f.write(chunk)
            f.close()
        
        with open(tempfile.gettempdir() + '/input.json') as fp:
            input_dict = json.load(fp)
            fp.close()

        os.remove(tempfile.gettempdir() + '/input.json')
        
        #extract income based on document type
        
        #EPF handheld or EPF statement:
        if doc_type == 'epf_hh' or doc_type == 'epf_st':
            
            #splitting the text at new lines to identify the line number of the table headers
            input_split = inputstring.split('\n')
    
            # line_list = []
    
            for line in input_split:
    
                if doc_type == 'epf_hh':
                
                    headers = re.findall(r'[B|8|R]u[l|i|7|1|t][a|o]n.*?[M|W|N|H][a|o]j[i|l|1]*?kan.*?P[e|o]k[e|o]rj[a|o]', line, re.IGNORECASE)
                    
                    if not headers:
                        headers = re.findall(r'[B|8|R]u[l|i|7|1|t][a|o]n.*?ll[a|o]j[i|l|1]*?kan.*?P[e|o]k[e|o]rj[a|o]', line, re.IGNORECASE)
                    
                    if headers:
                        # line_list.append(input_split.index(line))
                        line_num = input_split.index(line)
                        break
                
                if doc_type == 'epf_st':
                    
                    headers = re.findall(r'[B|8|R]u[l|i|7|1|t][a|o]n.*?Syer.*?Syer', line, re.IGNORECASE)
                    
                    if headers:
                        # line_list.append(input_split.index(line))
                        line_num = input_split.index(line)
                        break
            
            
            #identify the position of the header in the header line to extract the table below. Table width in simple terms
            text = input_split[line_num]

            if doc_type == 'epf_hh':
                
                try:
                    text_to_find = re.findall(r'[B|8|R]u[l|i|7|1|t][a|o]n.*?[M|W|N|H][a|o]j[i|l|1]*?kan.*?P[e|o]k[e|o]rj[a|o]', text, re.IGNORECASE)[0]
                except:
                    text_to_find = re.findall(r'[B|8|R]u[l|i|7|1|t][a|o]n.*?ll[a|o]j[i|l|1]*?kan.*?P[e|o]k[e|o]rj[a|o]', text, re.IGNORECASE)[0]
            
            if doc_type == 'epf_st':
                text_to_find = re.findall(r'[B|8|R]u[l|i|7|1|t][a|o]n.*?Syer.*?Syer', text, re.IGNORECASE)[0]

            text_pos = text.find(text_to_find)

            if text_pos < 5:
                text_start = 0
            else:
                text_start = text_pos - 5
    
            if text_pos + len(text_to_find) + 5 > len(text):
                text_end = text_pos + len(text_to_find)
            else:
                text_end = text_pos + len(text_to_find) + 5
    
            pf_list = []
            
            #plus_lines account for blank rows below headers
            plus_lines = 1
            
            if doc_type == 'epf_st':
                plus_lines = 4
            
            #loop through each line and extract the row values. Apply necessary validations
            for i in range(line_num + plus_lines, len(input_split)):
                
                cur_line = input_split[i]
                pfamt_line = cur_line[text_start:text_end]
    
                #if conditions not satisfied mark the end of the table
                if (doc_type == 'epf_hh' and re.findall(r'\d', pfamt_line)) or (doc_type == 'epf_st' and re.findall(r'Caruman.*?IWS', cur_line)) :
                    
                    len_1 = len(pfamt_line)
                    div = int(len(pfamt_line)/3)
    
                    mth = pfamt_line[0:div]
                    emp = pfamt_line[div+1:div*2+2]
                    wrk = pfamt_line[(div*2)+1:len(pfamt_line)]
                    
                    mth = mth.strip().replace('G','6').replace('/','7').replace('B','8')
                    emp = emp.strip().replace('G','6').replace('/','7').replace('B','8')
                    
                    if doc_type == 'epf_hh':
                        
                        emp = emp.strip().replace(' ','.').replace(',','.')
                        wrk = wrk.strip().replace(' ','.').replace(',','.')
                        
                        if emp.find('.') == -1:
                            b_d = emp[:-2]
                            a_d = emp[-2:]
                            emp = '{}.{}'.format(b_d, a_d)
                            
                        if wrk.find('.') == -1:
                            b_d = wrk[:-2]
                            a_d = wrk[-2:]
                            wrk = '{}.{}'.format(b_d, a_d)
                            
                        emp = re.sub(r'\..*', '.00', emp)
                        wrk = re.sub(r'\..*', '.00', wrk)
    
                        if str(emp[0]) == '0' and len(emp) > 5:
                            
                            lst = str(emp[1:])
                            emp = '8' + lst
                            
                        if str(wrk[0]) == '0' and len(wrk) > 5:
                            
                            lst = str(wrk[1:])
                            wrk = '8' + lst
                            
                    mth_wise = {}
    
                    mth_wise[str(mth)] = [str(emp), str(wrk)]
    
                    pf_list.append(mth_wise)
    
                else:
                    break
    
            #convert list to a json string
            if len(pf_list) > 12:
                pf_list = pf_list[len(pf_list)-12:]

            income_dict = {}
    
            for i in range(1,len(pf_list)+1):
    
                cur_record = pf_list[i-1]
                income_dict['month_{}'.format(str(i).zfill(2))] = str(cur_record.keys()[0])
                income_dict['employer_cont_{}'.format(str(i).zfill(2))] = str(cur_record.values()[0][0])
                income_dict['employee_cont_{}'.format(str(i).zfill(2))] = str(cur_record.values()[0][1])          
    
    
            income_dict['task_id'] = input_dict[u'records'][0][u'json_params'][u'data'][u'task_id']
            income_dict['staff_application'] = input_dict[u'records'][0][u'json_params'][u'data'][u'staff_application']
            
            return str(income_dict).replace('{','').replace('}','')

        #LHDN Statement:
        if doc_type == 'lhdn':
            
            #check if self employed or salaried from input params
            se = input_dict[u'records'][0][u'json_params'][u'data'][u'staff_application']
            
            #extract salaried income
            salaried = re.findall(r'Pendapatan.*?berkanun.*?perniagaan(.*?)\n', inputstring, re.IGNORECASE)[0]
            
            salaried_valid = [str(i) for i in re.findall(r'[[0-9]*]*',salaried) if str(i) != ''] #,*\.*\-*/*
            salaried = ''.join(salaried_valid)
            
            income_dict = {'lhdn_salaried' : salaried}
            
            #extract self employed income
            if se == 'true':
                
                self_employed = re.findall(r'Pendapatan.*?berkanun.*?penggajian(.*?)\n', inputstring, re.IGNORECASE)[0]
            
                self_employed_valid = [str(i) for i in re.findall(r'[[0-9]*]*',self_employed) if str(i) != ''] #,*\.*\-*/*
                self_employed = ''.join(self_employed_valid)
            
            #extract business loss for self employed income
                business_loss = re.findall(r'tahun.*?semasa(.*?)\n', inputstring, re.IGNORECASE)[0]
            
                business_loss_valid = [str(i) for i in re.findall(r'[[0-9]*]*',business_loss) if str(i) != ''] #,*\.*\-*/*
                business_loss = ''.join(business_loss_valid)
            
            else:
                self_employed = 'NA'
                business_loss = 'NA'
                
                income_dict = {'lhdn_salaried' : salaried, 'lhdn_self_employed' : self_employed, 'lhdn_business_loss' : business_loss}
            
            #generate income json
            income_dict['task_id'] = input_dict[u'records'][0][u'json_params'][u'data'][u'task_id']
            income_dict['staff_application'] = input_dict[u'records'][0][u'json_params'][u'data'][u'staff_application']
            
            return str(income_dict).replace('{','').replace('}','')
        
        #EA Statement
        if doc_type == 'ea':
            
            #extract total income
            income = re.findall(r'termasuk.*?gaji.*?lebih.*?masa(.*?)\n', inputstring, re.IGNORECASE)[0]
            income = income[-20:]
            income = str(re.findall(r'[^a-zA-Z0-9]*(.*)', income)[0])
            
            #extract commision and bonus
            com_bonus = re.findall(r'komisen.*?bonus(.*?)\n', inputstring, re.IGNORECASE)[0]
            com_bonus = com_bonus[-20:]
            com_bonus = str(re.findall(r'[^a-zA-Z0-9]*(.*)', com_bonus)[0])
            
            com_bonus_2 = re.findall(r'atau.*?elaun(.*?)\n', inputstring, re.IGNORECASE)[0]
            com_bonus_2 = com_bonus_2[-20:]
            com_bonus_2 = str(re.findall(r'[^a-zA-Z0-9]*(.*)', com_bonus_2)[0])
            
            #generate json
            income_dict = {'ea_income' : income, 'ea_com_bonus' : com_bonus, 'ea_com_bonus_2' : com_bonus_2}

            return str(income_dict).replace('{','').replace('}','')

    except Exception as e:

        return str(e)

#Register to Refiner Function---------------------------------------------------------------------------------------------------------

def register(name_to_fn):
    more_fns = {
        'income_extract': {
            'fn': income_extract,
            'ex': '',
            'desc': ''}
        }

    name_to_fn.update(more_fns)