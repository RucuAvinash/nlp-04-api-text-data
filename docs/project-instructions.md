# Project Instructions

## WEDNESDAY: Complete Workflow Phase 1

Follow the instructions in
[⭐ **Workflow: Apply Example**](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to complete:

1. Phase 1. **Start & Run** - copy the project and confirm it runs
2. Phase 2. **Change Authorship** - update the project to your name and GitHub account
3. Phase 3. **Read & Understand** - review the project structure and code

## FRIDAY/SUNDAY: Complete Workflow Phases 2-4

Again, follow the instructions above to complete:

1. Phase 4. **Make a Technical Modification**
2. Phase 5. **Apply the Skills to a New Problem**

## Phase 4 Suggestions

Make a small technical change that does not break the pipeline.
Choose any one of these (or a different modification as you like):

- Change the API endpoint to another JSONPlaceholder route (e.g., `/comments` instead of `/posts`)
- Modify the output file name (e.g., change `case_processed.csv`)
- Add a new derived column in the Transform stage (e.g., combine fields or compute lengths)
- Adjust logging messages to provide more detail about the pipeline stages
- Modify which fields are selected in the Transform stage

Confirm the script still runs successfully after your change.

## Phase 5 Suggestions

### Phase 5 Suggestion 1. New API Source (Directed)

Apply the same EVTL pipeline to a different JSON API.

You may use one of these example endpoints:

- https://jsonplaceholder.typicode.com/comments
- https://jsonplaceholder.typicode.com/users
- https://jsonplaceholder.typicode.com/albums

Steps:

- Update `config_case.py` (in your copied file) with the new API URL
- Run the pipeline
- Inspect the JSON structure in the Validate stage
- Update the Transform stage to extract relevant fields
- Run the pipeline again and confirm success

Then:

- Describe the structure of the JSON (list, dictionary, nested fields)
- Identify at least 3 useful fields and explain why they matter
- Explain what changes you made in Transform and why

### Phase 5 Suggestion 2. New API (Original Selection)

Apply this pipeline to a different API of your choice.

Examples include:

- public data APIs (weather, government data, etc.)
- APIs that require simple API keys (optional)

Steps:

- Update your copied `config` file with the new API
- Inspect the JSON structure carefully in Validate
- Modify Transform to extract useful fields
- Run the pipeline and confirm success

Then:

- Describe how the JSON structure differs from the original example
- Explain how you adapted your validation and transformation steps
- Identify one challenge you encountered and how you resolved it

## Key Skill Focus

As you work, focus on:

- how to fetch JSON data from an API
- how to inspect unknown JSON structures
- how to identify keys and data types
- how to extract only the fields you need
- how data moves through the EVTL pipeline

Your goal is to reuse the same pipeline on new data sources.

## Optional Enhancements

If time allows, consider:

- computing word counts or text lengths in Transform
- summarizing basic statistics (min, max, mean)
- exploring a second API endpoint for comparison

## Professional Communication

Remove instructor-provided content you no longer need in your project.

Make sure the title and narrative reflect your presentation.
Verify key files:

- README.md
- docs/ (source and hosted on GitHub Pages)
- src/ (pipeline and stage files)

Ensure your project clearly demonstrates:

- correct EVTL pipeline execution
- understanding of JSON structure
- ability to adapt the pipeline to new data
