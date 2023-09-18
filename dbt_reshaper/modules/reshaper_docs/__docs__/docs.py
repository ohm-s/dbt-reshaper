'''
This is a helper to extract all documentation from sql models, macros, tests, etc to create the documentation site
'''

DOC_BLOCK_START_KEY = "```doc"

def extract_dbt_docs(directory):
    from pprint import pprint
    import yaml
    try:
        import os
        pprint(directory)
        if os.path.isdir(directory) == False:
            print("Target is not a directory")
            return

        def serialize_to_yaml_file(data, file_path):
            with open(file_path, 'w') as file:
                yaml.dump(data, file)

        sql_dirs = os.walk(directory)
        for cdir, dirs, files in sql_dirs:
            for fname in files:
                if fname.endswith(".sql"):
                    doc_comment = extract_docs(cdir + "/" + fname)
                    if doc_comment is not None:
                        try:
                            doc_obj = yaml.safe_load(doc_comment)
                            doc_obj["name"] = os.path.splitext(os.path.basename(fname))[0]
                            if "description" not in doc_obj:
                                doc_obj["description"] = "Description not provided"
                            else:
                                doc_obj["description"] = doc_obj["description"].strip()
                                doc_obj["description"] = doc_obj["description"].replace(".\n", ".  \n")
                                if "arguments" in doc_obj:
                                    for argument in doc_obj['arguments']:
                                        if "description" in argument:
                                            argument["description"] = argument["description"].strip()
                                            argument["description"] = argument["description"].replace(".\n", ".  \n")
                            output_doc = {
                                "version": 2,
                                "macros": [
                                    doc_obj
                                ]
                            }
                            serialize_to_yaml_file(output_doc, cdir + "/" + doc_obj["name"] + ".yml")

                        except:
                            # implement error reporting
                            pass


    except Exception as ex:
        pprint(ex)

def extract_docs(sql_file):
    import re
    def extract_string_values(file_path):
        with open(file_path, 'r') as file:
            content = file.read()

        pattern = r"{#(.*?)#}"
        matches = re.findall(pattern, content, re.DOTALL)
        return matches

    dbt_comments = extract_string_values(sql_file)
    if len(dbt_comments) > 0:
        for comment in dbt_comments:
            if "```doc" in comment:
                comment = comment.strip()
                if comment.startswith("```doc") and comment.endswith("```"):
                    comment = comment.strip('`')
                    comment = comment[3:-1]
                    comment = comment.strip()
                    return comment
    return None


    # Recursive function to traverse the AST
def traverse_ast(node, get_type):
    try:
        if isinstance(node, get_type):
            # Perform any action with the extracted comment here
            print("Found comment:", node.comment)
        elif hasattr(node, "iter_child_nodes"):
            for child_node in node.iter_child_nodes():
                traverse_ast(child_node, get_type)
    except Exception as ex:
        from pprint import pprint
        pprint(ex)

