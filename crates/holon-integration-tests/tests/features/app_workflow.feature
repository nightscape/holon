Feature: Holon App Workflow
  As a Holon user
  I want to see my knowledge base rendered as widgets
  And have changes propagate in real-time

  Background:
    Given the backend is initialized

  Scenario: Initial app load with layout file
    Given the following "index.org" layout:
      """
      * Layout Root
      :PROPERTIES:
      :ID: layout-root
      :END:
      #+BEGIN_SRC prql
      from children
      filter content_type == "text"
      select {id, parent_id, content}
      render (list item_template:(text this.id))
      #+END_SRC
      ** Sidebar
      :PROPERTIES:
      :ID: sidebar
      :END:
      Sidebar content
      ** Main
      :PROPERTIES:
      :ID: main-view
      :END:
      Main view content
      """
    When I open the app
    Then the "all" widget should contain "sidebar"
    And the "all" widget should contain "main-view"

  Scenario: Journal content appears in widget
    Given the following "index.org" layout:
      """
      * Layout
      :PROPERTIES:
      :ID: layout-root
      :END:
      #+BEGIN_SRC prql
      from blocks
      filter content_type == "text"
      select {id, content}
      render (list item_template:(text this.content))
      #+END_SRC
      """
    And the following "journals/20251231.org" journal:
      """
      * Morning Thoughts
      :PROPERTIES:
      :ID: morning-thoughts
      :END:
      Starting the day with planning.
      """
    When I open the app
    Then within 5 seconds the "all" widget should contain "Morning Thoughts"

  Scenario: Creating a block updates the widget via CDC
    Given the following "index.org" layout:
      """
      * Layout
      :PROPERTIES:
      :ID: layout-root
      :END:
      #+BEGIN_SRC prql
      from blocks
      filter content_type == "text"
      select {id, content}
      render (list item_template:(text this.content))
      #+END_SRC
      """
    And a document "test.org" exists
    When I open the app
    And I execute operation "blocks.create" with:
      | key          | value              |
      | id           | new-idea-1         |
      | parent_id    | holon-doc://test.org |
      | content      | A brilliant new idea |
      | content_type | text               |
    Then within 5 seconds the "all" widget should contain "A brilliant new idea"

  Scenario: Editing org file triggers widget update
    Given the following "index.org" layout:
      """
      * Layout
      :PROPERTIES:
      :ID: layout-root
      :END:
      #+BEGIN_SRC prql
      from blocks
      filter content_type == "text"
      select {id, content}
      render (list item_template:(text this.content))
      #+END_SRC
      """
    And the following "journals/20251231.org" journal:
      """
      * Morning Thoughts
      :PROPERTIES:
      :ID: morning-thoughts
      :END:
      """
    When I open the app
    And I append to "journals/20251231.org":
      """
      * Afternoon Notes
      :PROPERTIES:
      :ID: afternoon-notes
      :END:
      Meeting summary here.
      """
    Then within 10 seconds the "all" widget should contain "Afternoon Notes"

  Scenario: Grandchildren virtual table queries nested blocks
    Given the following "index.org" layout:
      """
      * Layout
      :PROPERTIES:
      :ID: layout-root
      :END:
      #+BEGIN_SRC prql
      from grandchildren
      filter content_type == "text"
      select {id, content}
      render (list item_template:(text this.content))
      #+END_SRC
      ** Section A
      :PROPERTIES:
      :ID: section-a
      :END:
      *** Nested Item 1
      :PROPERTIES:
      :ID: nested-1
      :END:
      First nested item
      *** Nested Item 2
      :PROPERTIES:
      :ID: nested-2
      :END:
      Second nested item
      ** Section B
      :PROPERTIES:
      :ID: section-b
      :END:
      *** Nested Item 3
      :PROPERTIES:
      :ID: nested-3
      :END:
      Third nested item
      """
    When I open the app
    Then the "all" widget should contain "First nested item"
    And the "all" widget should contain "Second nested item"
    And the "all" widget should contain "Third nested item"

  Scenario: Roots virtual table queries top-level blocks
    Given the following "index.org" layout:
      """
      * Layout
      :PROPERTIES:
      :ID: layout-root
      :END:
      #+BEGIN_SRC prql
      from roots
      filter content_type == "text"
      select {id, content}
      render (list item_template:(text this.content))
      #+END_SRC
      """
    And the following "doc1.org" journal:
      """
      * Top Level 1
      :PROPERTIES:
      :ID: top-1
      :END:
      First top-level block
      """
    And the following "doc2.org" journal:
      """
      * Top Level 2
      :PROPERTIES:
      :ID: top-2
      :END:
      Second top-level block
      """
    When I open the app
    Then within 5 seconds the "all" widget should contain "First top-level block"
    And the "all" widget should contain "Second top-level block"

  Scenario: Documents table lists all org files
    Given the following "index.org" layout:
      """
      * Layout
      :PROPERTIES:
      :ID: layout-root
      :END:
      #+BEGIN_SRC prql
      from documents
      filter name != ""
      select {id, name}
      render (list item_template:(text this.name))
      #+END_SRC
      """
    And the following "notes.org" journal:
      """
      * Notes
      :PROPERTIES:
      :ID: notes-block
      :END:
      """
    And the following "tasks.org" journal:
      """
      * Tasks
      :PROPERTIES:
      :ID: tasks-block
      :END:
      """
    When I open the app
    Then within 5 seconds the "all" widget should contain "notes"
    And the "all" widget should contain "tasks"

  Scenario: Multi-panel layout with different queries
    Given the following "index.org" layout:
      """
      * Layout
      :PROPERTIES:
      :ID: layout-root
      :END:
      #+BEGIN_SRC prql
      from grandchildren
      filter content_type == "source" && source_language == "prql"
      select {id, parent_id, content, content_type, source_language}
      render (columns item_template:(render_block this))
      #+END_SRC
      ** Left Panel
      :PROPERTIES:
      :ID: left-panel
      :END:
      #+BEGIN_SRC prql
      from documents
      select {id, name}
      render (list item_template:(text this.name))
      #+END_SRC
      ** Right Panel
      :PROPERTIES:
      :ID: right-panel
      :END:
      #+BEGIN_SRC prql
      from blocks
      filter content_type == "text"
      select {id, content}
      render (list item_template:(text this.content))
      #+END_SRC
      """
    And the following "myfile.org" journal:
      """
      * My Content
      :PROPERTIES:
      :ID: my-content
      :END:
      Some text here
      """
    When I open the app
    Then within 5 seconds the "all" widget should contain "myfile"
    And the "all" widget should contain "Some text here"
