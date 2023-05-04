package storage

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Storage Хранилище данных.
type Storage struct {
	db *pgxpool.Pool
}

// New Конструктор, принимает строку подключения к БД.
func New(constr string) (*Storage, error) {
	db, err := pgxpool.New(context.Background(), constr)
	if err != nil {
		return nil, err
	}
	s := Storage{
		db: db,
	}
	return &s, nil
}

// Task Задача.
type Task struct {
	ID         int
	Opened     int64
	Closed     int64
	AuthorID   int
	AssignedID int
	Title      string
	Content    string
}

// Tasks возвращает список задач из БД.
func (s *Storage) Tasks() ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT 
			id,
			opened,
			closed,
			author_id,
			assigned_id,
			title,
			content
		FROM tasks
		ORDER BY id;`,
	)
	if err != nil {
		return nil, err
	}
	var tasks []Task
	// итерирование по результату выполнения запроса
	// и сканирование каждой строки в переменную
	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)
		if err != nil {
			return nil, err
		}
		// добавление переменной в массив результатов
		tasks = append(tasks, t)

	}
	// ВАЖНО не забыть проверить rows.Err()
	return tasks, rows.Err()
}

// NewTask создаёт новую задачу и возвращает её id.
func (s *Storage) NewTask(t Task) (int, error) {
	var id int
	err := s.db.QueryRow(context.Background(), `
		INSERT INTO tasks (author_id, assigned_id, title, content)
		VALUES ($1, $2, $3, $4) RETURNING id;
		`,
		t.AuthorID,
		t.AssignedID,
		t.Title,
		t.Content,
	).Scan(&id)
	return id, err
}

// TaskByAuthor возвращает список задач по автору
func (s *Storage) TaskByAuthor(authorID int) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT 
			id,
			opened,
			closed,
			author_id,
			assigned_id,
			title,
			content
		FROM tasks
		WHERE author_id = $1
		ORDER BY id;`,
		authorID,
	)

	if err != nil {
		return nil, err
	}

	var tasks []Task

	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)

		if err != nil {
			return nil, err
		}

		tasks = append(tasks, t)
	}

	return tasks, rows.Err()
}

// TaskByLabel возвращает список задач по метке
func (s *Storage) TaskByLabel(labelID int) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT 
			id,
			opened,
			closed,
			author_id,
			assigned_id,
			title,
			content
		FROM tasks
		JOIN task_labels ON tasks.id = task_labels.task_id
		JOIN labels ON task_labels.label_id = labels.id
		WHERE labels.id = $1
		ORDER BY id;`,
		labelID,
	)

	if err != nil {
		return nil, err
	}

	var tasks []Task

	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)

		if err != nil {
			return nil, err
		}

		tasks = append(tasks, t)
	}

	return tasks, rows.Err()
}

// UpdateTaskByID обновляет задачу по ID
func (s *Storage) UpdateTaskByID(t Task) error {
	_, err := s.db.Exec(context.Background(), `
			UPDATE tasks
			SET content = $1
			WHERE id = $2;`,
		t.Content,
		t.ID,
	)

	if err != nil {
		return err
	}
	return nil
}

// DeleteTaskByID удаляет задачу по ID
func (s *Storage) DeleteTaskByID(taskID int) error {
	_, err := s.db.Exec(context.Background(), `
			DELETE FROM tasks
			WHERE id = $1;`,
		taskID,
	)

	if err != nil {
		return err
	}
	return nil
}
