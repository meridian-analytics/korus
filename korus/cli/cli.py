import inquirer

#https://python-inquirer.readthedocs.io/en/latest/usage.html#question-types

questions = [
    inquirer.List(
        name="table", 
        message="Select a table",
        choices=["deployment", "annotation", "Exit"]    
    )
]

answers = inquirer.prompt(questions)


print(answers)
