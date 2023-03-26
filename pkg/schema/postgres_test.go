package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_parsePostgresSchema(t *testing.T) {
	tests := []struct {
		name        string
		schemaInput string
		testFunc    func(t *testing.T, res map[string]Table, err error)
	}{
		{
			name: "empty schema",
			testFunc: func(t *testing.T, res map[string]Table, err error) {
				require.NoError(t, err)
				require.Empty(t, res)
			},
		},
		{
			name: "single table",
			schemaInput: `
CREATE TABLE public.users (
    id integer NOT NULL,
	name text NOT NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
);
`,
			testFunc: func(t *testing.T, res map[string]Table, err error) {
				require.NoError(t, err)
				require.Equal(t, map[string]Table{
					"users": {
						Name: "users",
						Columns: map[string]Column{
							"id": {
								Name: "id",
								Type: "pg_catalog.int4",
							},
							"name": {
								Name: "name",
								Type: "text",
							},
						},
					},
				}, res)
			},
		},
		{
			name: "multiple tables",
			schemaInput: `
CREATE TABLE public.users (
    id integer NOT NULL,
    	name text NOT NULL,
    		CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.posts (
    id integer NOT NULL,
    	title text NOT NULL,
    		CONSTRAINT posts_pkey PRIMARY KEY (id)
);
`,
			testFunc: func(t *testing.T, res map[string]Table, err error) {
				require.NoError(t, err)
				require.Equal(t, map[string]Table{
					"users": {
						Name: "users",
						Columns: map[string]Column{
							"id": {
								Name: "id",
								Type: "pg_catalog.int4",
							},
							"name": {
								Name: "name",
								Type: "text",
							},
						},
					},
					"posts": {
						Name: "posts",
						Columns: map[string]Column{
							"id": {
								Name: "id",
								Type: "pg_catalog.int4",
							},
							"title": {
								Name: "title",
								Type: "text",
							},
						},
					},
				}, res)
			},
		},
		{
			name: "view",
			schemaInput: `
CREATE VIEW public.users_posts AS
    	SELECT *, u.id AS user_id, u.name AS user_name, user_property, p.id AS post_id, posts.title, post_property, count(*) as count
    	FROM public.users u
    	JOIN public.posts p ON u.id = p.user_id;
`,
			testFunc: func(t *testing.T, res map[string]Table, err error) {
				require.NoError(t, err)
				require.Equal(t, map[string]Table{
					"users_posts": {
						Name: "users_posts",
						Columns: map[string]Column{
							"user_id": {
								Name: "user_id",
							},
							"user_name": {
								Name: "user_name",
							},
							"user_property": {
								Name: "user_property",
							},
							"post_id": {
								Name: "post_id",
							},
							"title": {
								Name: "title",
							},
							"post_property": {
								Name: "post_property",
							},
							"count": {
								Name: "count",
							},
						},
						ReadOnly: true,
					},
				}, res)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := parsePostgresSchema(tt.schemaInput)
			if tt.testFunc == nil {
				t.Fatalf("testFunc is nil")
				return
			}
			tt.testFunc(t, res, err)
		})
	}
}
