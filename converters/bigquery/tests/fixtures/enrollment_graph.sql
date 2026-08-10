CREATE OR REPLACE PROPERTY GRAPH enrollment_graph
  NODE TABLES (
    `campus.public.student` AS student
      KEY(student_id)
      DEFAULT LABEL OPTIONS(description="A person enrolled at the school", synonyms=["students", "learners"])
      PROPERTIES(
        student_id OPTIONS(description="Unique identifier for the student", synonyms=["student number"]),
        student_name OPTIONS(description="Student full name", synonyms=["name"])
      ),
    `campus.public.course` AS course
      KEY(course_id)
      DEFAULT LABEL OPTIONS(description="A course students can enroll in", synonyms=["courses", "classes"])
      PROPERTIES(
        course_id OPTIONS(description="Unique identifier for the course", synonyms=["course number"]),
        title OPTIONS(description="Course title", synonyms=["course name"])
      )
  )
  EDGE TABLES (
    `campus.public.enrollment` AS enrolled_in
      KEY(s_id, c_id)
      SOURCE KEY (s_id) REFERENCES student (student_id)
      DESTINATION KEY (c_id) REFERENCES course (course_id)
      DEFAULT LABEL OPTIONS(description="A student's enrollment in a course", synonyms=["takes", "registered for"])
      PROPERTIES(
        grade OPTIONS(description="Final grade the student earned", synonyms=["mark", "score"]),
        enrolled_on OPTIONS(description="Date the student enrolled in the course", synonyms=["enrollment date"])
      )
  )
  OPTIONS(description="Students and the courses they enroll in", synonyms=["course enrollment graph", "registrations"]);
