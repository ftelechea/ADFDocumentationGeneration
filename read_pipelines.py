"""Reads a single pipeline file from Azure Data Factory and appends the relevant parts 
  of its contents to an existing Markdown file. 

This file is part of ADF Documentation Generation.

ADF Documentation Generation is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

# Module: read_datasets.py

import logging
import json

def read_pipeline(pipeline_file_name, markdown_file_name):
  """Reads a single pipeline file from Azure Data Factory and appends the relevant parts 
  of its contents to an existing Markdown file. 

  The following information will be contained in the resulting markdown file:

  * the name of the pipeline
  * the description of the pipeline if available
  * a list of its activities
  * a list of its dependencies together with the dependency condition

  Parameters
  ----------
  pipeline_file_name : str
  The full path of the ADF json file to be read
  markdown_file_name : str
  The full path of the markdown file to which the contents will be appended
  """

  # open /create markdown file
  md = open(markdown_file_name, 'a')
  logging.info('\t reading %s' % (pipeline_file_name))

  with open(pipeline_file_name) as json_file:
    pipelines_data = json.load(json_file)
    md.write('\n\n ## %s \n' % pipelines_data['name'])
    
    # get properties section of the pipeline json file 
    properties = pipelines_data.get('properties', {})

    if 'description' in properties:
      # get pipeline description
      pipeline_description = properties['description']
      
      # write line in markdown file
      md.write(f'\n **Description:** {pipeline_description} \n')
      
      md.write('\n\n ### Steps \n')
    
    # begin Mermaid code
    mermaid_code = "```mermaid\ngraph LR;\n"
    
    # iterate over activities  
    for act in properties['activities']:
      # name and type activity
      activity_name = act['name']
      tipo_actividad = act['type']
      md.write(f'\n - **Name:** {activity_name} \n')
      md.write(f'\n   **Type:** {tipo_actividad} \n')
      
      # add activities to mermaid code
      mermaid_code += f"{activity_name.replace(' ', '_')}({activity_name});\n"
      
      #  if exist activity description. add to markdown file 
      if 'description' in act:
        act_description = act['description']
        md.write(f'\n   **Description:** {act_description} \n')
        
      # if exists dependencies add to markdown and mermaid code
      if len(act['dependsOn']) > 0:
        md.write('\n   **Dependencies:**')
        for dep in act['dependsOn']:
          md.write('\n   * [{0}]({1}) ({2}) \n'.format(dep['activity'], '#'+dep['activity'].replace(' ', '-'), dep['dependencyConditions'][0]))
          mermaid_code += f"{dep['activity'].replace(' ', '_')} --> {activity_name.replace(' ', '_')};\n"
      
      # If activity is a Copy, extract SQL query of the source
      if act['type'] == "Copy":
        # write source info type
        source_info = act['typeProperties']['source']
        source_info_type = source_info['type']
        md.write(f'\n   **Data Source Type:** {source_info_type} \n')
        
        # write sqlReaderQuery 
        value_sql = source_info['sqlReaderQuery']['value']
        md.write(f'\n   **SQL Statement:**  \n')
        md.write(f'\n   ```SQL \n   {value_sql} \n   ```')
        
    # fin de actividades
    mermaid_code +=  ' ```'
    
    # add mermaid diagram to markdown file
    md.write('\n\n ### Activities Diagram \n')
    md.write(f'\n {mermaid_code}')

    # if exist parameter in pipeline, add to the parameters table in markdown
    if 'parameters' in properties:
      # Header Table
      md.write('\n\n ### Parameters \n')
      md.write('\n | Name | Type | Default Value |')
      md.write('\n |---|---|---|')
      
      # get parameter section
      parameters = properties.get('parameters', {})
      # Iterate over each parameter
      for parameter, info in parameters.items():
          parameter_name = parameter
          parameter_type = info.get('type')
          default_value = info.get('defaultValue')
          md.write(f'\n |{parameter_name}|{parameter_type}|{default_value}|')
  
  # close markdown file
  md.close()
  
# end read_pipeline method
