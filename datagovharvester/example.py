#!/usr/bin/env python3

"""
Example module for datagov harvesting
"""

def hello(name):
    return "Hello " + name + "!"

def main():
    name = input("Please input your name: ")
    output = hello(name)
    
    print(output)

if __name__ == "__main__":
    main()
