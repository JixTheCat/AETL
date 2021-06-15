from etl import etl

import multiprocessing as mp
import pandas as pd
import pytest

from etl import etl


def square(input):
    return input*input


def print_input(input):
    print(input)


def etl_easy():
    task_list = [1, 2, 3, 5, 7, 11]
    etl(task_list=task_list,
        extract=square,
        transform=square,
        load=print_input)

####################################################################

def extract(fish_and_chips, foo, blue, two):
    if (foo + str(fish_and_chips)) == 'bar7':
        df = pd.DataFrame([['shark', 7, 'yellow']],
                          columns=['fish', 'number', 'colour'])
    elif fish_and_chips == two:
        df = pd.DataFrame([['clown fish', 2, 'orange']],
                           columns=['fish', 'number', 'colour'])
    elif fish_and_chips == 19:
        df = pd.DataFrame([['trout', 23, 'green']],
                           columns=['fish', 'number', 'colour'])
    else:
        df = pd.DataFrame([['just a fish', fish_and_chips, blue]],
                           columns=['fish', 'number', 'colour'])
    return df


def transform(dolphins, blah, ok):
    dolphins.number = dolphins.number * dolphins.number
    if str(dolphins.fish[0]) == 'just a fish':
        dolphins.fish = 'dinner'
    if int(dolphins.number[0]) in [1,2]:
        dolphins.fish = ok()
    return dolphins


def load(spahgetti, some_variable):
    global Global
    global manager
    if int(spahgetti.number[0]) == 9:
        spahgetti.number[0] = some_variable
    #lock = Global.Lock()
    #lock.acquire()
    Global.df = Global.df.append(spahgetti)
    #lock.release()
    return Global.df


def a_function():
    return 'fishy function'


manager = mp.Manager()
Global = manager.Namespace()
#Global.df = pd.DataFrame(columns=['number','fish','colour'])
Global.df = pd.DataFrame(columns=['number','fish','colour'])

def etl_full():
    # setup
    global Global
    df = pd.DataFrame([[99, 'dolphin', 'aqua']],columns=['number','fish','colour'])
    #task_list = [1, 2, 3, 5, 7, 11, 13, 17, 19]
    task_list = [1, 19, 3, 5, 7, 11, 13, 17, 2]
    extract_kwargs = {'foo': 'bar', 'blue': 'red', 'two': 2}
    transform_kwargs = {'blah': 4, 'ok': a_function}
    load_kwargs = {'some_variable': 1337}
    extract_in_var = 'fish_and_chips'
    transform_in_var = 'dolphins'
    load_in_var = 'spahgetti'
    # 1, 17, ?3?, 2, 13, 7, 23, 11
    # Run
    etl(task_list=task_list,
            extract=extract,
            transform=transform,
            load=load,
            extract_kwargs=extract_kwargs,
            transform_kwargs=transform_kwargs,
            load_kwargs=load_kwargs,
            extract_in_var=extract_in_var,
            transform_in_var=transform_in_var,
            load_in_var=load_in_var)

    # Assert
    fish_test = set(['dinner',
                     'shark',
                     'trout',
                     'clown fish',
                     'fishy function'])

    number_test = set([1,
                       4,
                       1337,
                       25,
                       49,
                       121,
                       169,
                       289,
                       529])

    colour_test = set(['yellow',
                       'orange',
                       'green',
                       'red'])

    assert fish_test == set(Global.df.fish)

    assert number_test == set(Global.df.number)

    assert colour_test == set(Global.df.colour)
    

def test_etl_easy_one():
    etl_easy()


def test_etl_full():
    etl_full()
