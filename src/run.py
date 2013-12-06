#!/usr/bin/python
"""
   Copyright (c) 2013, Yue.YE, ACT, Beihang University.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import codegen
import sys
import os

if __name__ == "__main__":
    if len(sys.argv) < 6:
        exit(0)
    schema = sys.argv[1]
    xmlfile = sys.argv[2]
    queryname = sys.argv[3]
    input_dir = sys.argv[4]
    output_dir = sys.argv[5]
    codegen.gen(schema, xmlfile, queryname, input_dir, output_dir)
