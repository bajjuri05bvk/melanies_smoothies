# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
import pandas as pd

# Write directly to the app
st.title(f":cup_with_straw: Customize Your Smoothie!:cup_with_straw:")
st.write(
  """Choose the fruits you want in your custom Smoothie.
  """
)


name_on_order = st.text_input("Name on Smoothie:")
st.write("The name on your Smoothie will be:", name_on_order)

cnx = st.connection("snowflake")
session= cnx.session()

my_dataframe = session.table("smoothies.public.fruit_options").select(col('fruit_name'))
# st.dataframe(data=my_dataframe, use_container_width=True)

# convert the snowpark dataframe into pandas dataframe so we can use LOC function
pd_df=my_dataframe.to_pandas()
#st.dataframe(pd_df)
#st.stop

ingredients_list = st.multiselect(
    "Choose upto 5 ingredients"
    ,my_dataframe
    ,max_selections=5
)

if ingredients_list:
   # st.write(ingredients_list)
   # st.text(ingredients_list)

    ingredients_string=''

    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '
      
        # Filter for matching fruit
        #search_on= pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        # Check if the user picked a fruit
        if fruit_chosen:
                # Make sure columns exist
             required_cols = {"FRUIT_NAME", "SEARCH_ON"}
             if not required_cols.issubset(pd_df.columns):
                   st.error(f"Expected columns {required_cols}, but got {list(pd_df.columns)}")
                   st.stop()
            
                # Filter safely (case-insensitive, NaN-safe)
        filtered = pd_df.loc[pd_df['FRUIT_NAME'].str.lower().fillna('') == fruit_chosen.lower(),'SEARCH_ON']
            
         if not filtered.empty:
               search_on = filtered.iloc[0]
         else:
               st.error(f"No match found for '{fruit_chosen}'.")
               st.stop()
         else:
                st.warning("Please choose a fruit first.")
                st.stop()



        st.subheader(fruit_chosen +'Nutrition Information')
        smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/" +fruit_chosen)
        sf_df= st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)

        
    #st.write(ingredients_string)

    my_insert_stmt = """ insert into smoothies.public.orders(ingredients,name_on_order)
            values ('""" + ingredients_string + """','"""+name_on_order+"""')"""

    #st.write(my_insert_stmt)
    #st.stop
    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        
        st.success(f'Your Smoothie is ordered , {name_on_order}!', icon="✅")


