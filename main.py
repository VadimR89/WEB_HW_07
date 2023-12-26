from operator import and_

from sqlalchemy import func, desc, select
from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_one():
    """Знайти 5 студентів із найбільшим середнім балом з усіх предметів."""
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_two(discipline_id: int):
    """Знайти студента із найвищим середнім балом з певного предмета."""
    result = session.query(Discipline.name,
                           Student.fullname,
                           func.round(func.avg(Grade.grade), 2).label('avg_grade')
                           ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Student.id, Discipline.name) \
        .order_by(desc('avg_grade')) \
        .limit(1).all()
    return result


def select_three(discipline_id: int):
    """Знайти середній бал у групах з певного предмета."""
    result = session.query(Discipline.name,
                           Group.name,
                           func.round(func.avg(Grade.grade), 2).label('avg_grade')
                           ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Group.id, Discipline.name) \
        .order_by(desc('avg_grade')) \
        .all()
    return result


def select_four():
    """Знайти середній бал на потоці (по всій таблиці оцінок)."""
    result = session.query(func.ROUND(func.AVG(Grade.grade), 2)).scalar()

    return result


def select_five(teacher_id: int):
    """Знайти які курси читає певний викладач."""
    result = session.query(Discipline.name, Teacher.fullname) \
        .join(Teacher, Teacher.id == Discipline.teacher_id) \
        .filter(Teacher.id == teacher_id) \
        .all()

    return result


def select_six(group_id: int):
    """Знайти список студентів у певній групі."""
    result = session.query(Student.fullname, Group.name) \
        .join(Group, Student.group_id == Group.id) \
        .filter(Group.id == group_id) \
        .order_by(Student.fullname.asc()) \
        .all()

    return result


def select_seven(group_id: int, discipline_id: int):
    """Знайти оцінки студентів у окремій групі з певного предмета."""
    result = session.query(Student.fullname, Grade.grade) \
        .join(Group, Student.group_id == Group.id) \
        .join(Grade, Student.id == Grade.student_id) \
        .join(Discipline, Grade.discipline_id == Discipline.id) \
        .filter(Group.id == group_id, Discipline.id == discipline_id) \
        .all()

    return result


def select_eight():
    """Знайти середній бал, який ставить певний викладач зі своїх предметів."""
    result = session.query(Teacher.fullname, func.ROUND(func.AVG(Grade.grade), 0).label('average_grade')) \
        .join(Discipline, Teacher.id == Discipline.teacher_id) \
        .join(Grade, Discipline.id == Grade.discipline_id) \
        .group_by(Teacher.id) \
        .all()

    return result


def select_nine(student_id: int):
    """Знайти список курсів, які відвідує певний студент."""
    result = session.query(Discipline.name, Student.fullname) \
        .join(Grade, Discipline.id == Grade.discipline_id) \
        .join(Student, Grade.student_id == Student.id) \
        .filter(Student.id == student_id) \
        .order_by(Discipline.id.desc()) \
        .all()

    return result


def select_ten(student_id: int, teacher_id: int):
    """Список курсів, які певному студенту читає певний викладач."""
    result = session.query(Discipline.name, Student.fullname, Teacher.fullname) \
        .join(Teacher, Discipline.teacher_id == Teacher.id) \
        .join(Grade, Discipline.id == Grade.discipline_id) \
        .join(Student, Grade.student_id == Student.id) \
        .filter(Teacher.id == teacher_id, Student.id == student_id) \
        .all()

    return result


if __name__ == '__main__':
    print(select_one())
    print(select_two(1))
    print(select_three(1))
    print(select_four())
    print(select_five(1))
    print(select_six(6))
    print(select_seven(1, 1))
    print(select_eight())
    print(select_nine(1))
    print(select_ten(1, 1))
