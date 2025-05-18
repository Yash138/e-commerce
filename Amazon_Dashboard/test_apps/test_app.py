import streamlit as st

st.title('My First Streamlit App')
st.write("Hello, let's learn how to build a Streamlit app together!")

user_input = st.text_input("Enter some text")
st.write('The user entered:', user_input)


slider_val = st.slider('Select a number', 0, 100)
st.write('Slider value:', slider_val)


import pandas as pd
import numpy as np

df = pd.DataFrame(np.random.randn(10, 2), columns=['A', 'B'])
st.write(df)


st.header("Header")

st.subheader("Subheader")

st.markdown("**Markdown text**")

st.sidebar.title("Sidebar Title") # for sidebar elements

st.caption("Caption text")

st.code("print('Hello')")

st.latex(r'''a^2 + b^2 = c^2''')


import matplotlib.pyplot as plt

fig, ax = plt.subplots()
ax.plot([1, 2, 3], [4, 5, 6])
st.pyplot(fig)


import streamlit as st
import pandas as pd
import numpy as np
from time import sleep

st.title("Simple Data Dashboard")

df = pd.DataFrame(np.random.randn(20, 3), columns=['A', 'B', 'C'])
option = st.selectbox('Which column?', df.columns)
st.line_chart(df[option])


text_obj = st.text('Loading...')
sleep(5)
text_obj.write('Done!')