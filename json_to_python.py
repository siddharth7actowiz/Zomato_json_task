import json
import re
from datetime import *
structured_data = dict()
#raw json file path
raw_json = "D:\\Siddharth\\JSON\\input_zomato_data.json"

#filename for structured_json
f_name="ZOMATO"
today=datetime.today()
todays_date=datetime.strftime(today,"%Y_%m_%d")
file_name=f"{f_name}_{todays_date}.json"


#function to read json and return raw_json dict
def read_json(raw_json):
    with open(raw_json, "r") as file:
        python_obj = json.load(file)
        print(type(python_obj))
        raw_json_dict = dict(python_obj)
        return raw_json_dict    
    
#function to parse raw__dict and return structured dict
def json_parser(raw_dict):
        LIC_NO=raw_dict.get("page_data").get("order").get("menuList").get("fssaiInfo").get("text")
        match = re.search(r'(\d+)', LIC_NO).group()
        base_path=raw_dict.get("page_info")
        structured_data["restaurant_id"]=base_path.get("resId")
        structured_data["restaurant_name"]=base_path.get("ogTitle")
        

        structured_data["restaurant_contact"] = [raw_dict.get("page_data").get("sections").get("SECTION_RES_CONTACT").get("phoneDetails").get("phoneStr")]  
        structured_data["fssai_licence_number"]=int(match)
        structured_data["address_info"]={
             "full_address":raw_dict.get("page_data").get("sections").get("SECTION_RES_CONTACT").get("address"),
             "region":raw_dict.get("page_data").get("sections").get("SECTION_RES_CONTACT").get("country_name"),
             "city":raw_dict.get("page_data").get("sections").get("SECTION_RES_CONTACT").get("city_name"),
             "pincode":raw_dict.get("page_data").get("sections").get("SECTION_RES_CONTACT").get("zipcode"),

             "state":"Gujrat"
             }



       

        structured_data["cuisines"]=[
             {"name":i.get("name"),
                "url":i.get("url")
             }
             for i in raw_dict.get("page_data").get("sections").get("SECTION_RES_HEADER_DETAILS").get("CUISINES")
        ]

        #Restro openning and Closing timing
        days=["monday","tuesdy","wednsday","thursday","friday","saturday","sunday"]
        open_time=raw_dict["page_data"]["sections"]["SECTION_BASIC_INFO"]["timing"]["customised_timings"]["opening_hours"][0]["timing"].split()[0]
        close_time=raw_dict["page_data"]["sections"]["SECTION_BASIC_INFO"]["timing"]["customised_timings"]["opening_hours"][0]["timing"].split()[-1]
        if open_time in ["12noon","noon","NOON","12NOON"]:
            open_time="12pm"
        
        structured_data["timings"]={
            
            day:{"open":open_time,"close":close_time}
            for day in days
            

            
        }
             
    
       
       


        store=[]
        for i in raw_dict["page_data"]["order"]["menuList"]["menus"]:
            
            for j in i["menu"]["categories"]:
                temp_dict=dict()
                temp_dict['category_name'] = i["menu"]["name"]
                temp_dict['items'] = [
                {
                    
                    "item_id": item["item"]["id"], 
                    "item_name": item["item"]["name"],
                    "item_slugs": item["item"]["tag_slugs"],
                    "item_description":item["item"]["desc"],
                    "is_veg":  item["item"]["dietary_slugs"][0] == "veg" 
                } 
                for item in j["category"]["items"] 
                ]
                store.append(temp_dict)
        
        structured_data["menu_categories"] = store
        print(structured_data)
        return structured_data





#function for structured_json
def export_structured_data_func(res):
    with open(file_name,"w") as file:
         file.write(json.dump(res,file,indent=4))
    



# Function call to load jsion 
raw_dict = read_json(raw_json)

#Function Call to Parse Json
res=json_parser(raw_dict)


#Function call to create json file
export_structured_data_func(res)
