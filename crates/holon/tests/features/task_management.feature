Feature: Task Management
  As a user
  I want to manage tasks
  So that I can organize my work

  Scenario: Get all tasks
    Given the task store has default tasks
    When I get all tasks
    Then I should see 2 root tasks
    And the first task should have the title "Build Rusty Knowledge MVP"

  Scenario: Add a new root task
    Given the task store has default tasks
    When I add a task with title "Write documentation"
    Then I should see 3 root tasks
    And the last task should have the title "Write documentation"

  Scenario: Add a child task
    Given the task store has default tasks
    When I add a task with title "Test sync" as a child of task "2"
    Then task "2" should have 1 child
    And the child task should have the title "Test sync"

  Scenario: Toggle task completion
    Given the task store has default tasks
    When I toggle task "1"
    Then task "1" should be completed

  Scenario: Toggle task back to incomplete
    Given the task store has default tasks
    And I toggle task "1"
    When I toggle task "1" again
    Then task "1" should not be completed

  Scenario: Update task title
    Given the task store has default tasks
    When I update task "2" with title "Implement Loro sync"
    Then task "2" should have the title "Implement Loro sync"

  Scenario: Delete a task
    Given the task store has default tasks
    When I delete task "2"
    Then I should see 1 root task
    And I should not see task "2"

  Scenario: Move task to root level
    Given the task store has default tasks
    When I move task "1-1" to root level at index 0
    Then I should see 3 root tasks
    And task "1-1" should be at root level

  Scenario: Move task under a new parent
    Given the task store has default tasks
    And I add a task with title "Backend tasks"
    When I move task "1-2" under task "2" at index 0
    Then task "2" should have 1 child
