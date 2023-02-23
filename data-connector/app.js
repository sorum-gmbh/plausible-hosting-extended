const { createClient } = require('@clickhouse/client')
const express = require('express')
const queue = require('express-queue')

const app = express()
const port = 3123

app.use(express.json())
app.use(queue({ activeLimit: 1, queuedLimit: -1 }))

function authMiddleware(req, res, next) {
	if (req.headers['api-key'] === process.env.DATA_CONNECTOR_API_KEY) {
		next()
	} else {
		res.status(401).send(`Unauthorized`)
	}
}

app.use(authMiddleware)

const client = createClient({
	host: 'http://127.0.0.1:8123',
	username: 'default',
	password: ''
})

const DIMENSIONS = ['visitors', 'events', 'pageviews']

app.post('/stats', async (req, res) => {
	try {
		const fields = req.body.fields ?? []
		const domain = req.body.domain
		const startDate = req.body.startDate
		const endDate = req.body.endDate

		console.log(`${new Date().toISOString()} /stats query (${domain}): ${fields.join(', ')}`)

		const query_fields = fields
			.map(field => {
				switch (field) {
					case 'visitors':
						return 'COUNT(DISTINCT user_id) AS visitors'
					case 'events':
						return 'COUNT() as events'
					case 'pageviews':
						return "COUNT() filter (where plausible_events_db.events.name = 'pageview') as pageviews"
					case 'event_name':
						return 'name as event_name'
					case 'entry_page':
						return 'session_entries.entry_page'
					case 'utm_medium':
						return 'session_entries.utm_medium'
					case 'utm_source':
						return 'session_entries.utm_source'
					case 'utm_campaign':
						return 'session_entries.utm_campaign'
					case 'utm_term':
						return 'session_entries.utm_term'
					case 'utm_content':
						return 'session_entries.utm_content'
					case 'date':
						return 'toString(toYYYYMMDD(timestamp)) as date'
					default:
						return field
				}
			})
			.join(', ')

		const group_by_fields = fields
			.filter(field => !DIMENSIONS.includes(field))
			.map(field => {
				switch (field) {
					case 'event_name':
						return 'name'
					case 'date':
						return 'toString(toYYYYMMDD(timestamp))'
					case 'entry_page':
						return 'session_entries.entry_page'
					case 'utm_medium':
						return 'session_entries.utm_medium'
					case 'utm_source':
						return 'session_entries.utm_source'
					case 'utm_campaign':
						return 'session_entries.utm_campaign'
					case 'utm_term':
						return 'session_entries.utm_term'
					case 'utm_content':
						return 'session_entries.utm_content'
					default:
						return field
				}
			})
			.join(', ')

		const where = `WHERE domain='${domain}' AND timestamp between '${startDate} 00:00:00' and '${endDate} 23:59:59'`

		const addEntryPageQuery = fields.reduce((a, c) => {
			if (a) return true
			if (c.match(/utm|entry_page/)) {
				return true
			}
			return false
		}, false)

		const entry_page_query_fields = fields
			.filter(field => field.match(/utm|entry_page/))
			.map(field => {
				switch (field) {
					case 'entry_page':
						return 'plausible_events_db.events.pathname as entry_page'
					case 'utm_medium':
						return 'plausible_events_db.events.utm_medium as utm_medium'
					case 'utm_source':
						return 'plausible_events_db.events.utm_source as utm_source'
					case 'utm_campaign':
						return 'plausible_events_db.events.utm_campaign as utm_campaign'
					case 'utm_term':
						return 'plausible_events_db.events.utm_term as utm_term'
					case 'utm_content':
						return 'plausible_events_db.events.utm_content as utm_content'
					default:
						return ''
				}
			})
			.join(', ')

		const query = `\
		${
			addEntryPageQuery
				? `\
				WITH session_entries as (
					WITH session_start AS (
						SELECT session_id, MIN(timestamp) as session_start
						FROM plausible_events_db.events
						${where}
						GROUP BY session_id
					)
					SELECT session_id, session_start, ${entry_page_query_fields}
					FROM session_start
					JOIN plausible_events_db.events
					ON session_start.session_id = plausible_events_db.events.session_id AND session_start.session_start = plausible_events_db.events.timestamp
					${where}
				)`
				: ''
		}
		SELECT ${query_fields}
		FROM plausible_events_db.events
		${
			addEntryPageQuery
				? 'JOIN session_entries ON plausible_events_db.events.session_id = session_entries.session_id'
				: ''
		}
		${where}
		${group_by_fields.length > 0 ? `GROUP BY ${group_by_fields}` : ''}
		${fields.includes('date') ? 'ORDER BY toString(toYYYYMMDD(timestamp))' : ''}
		`

		const result = await client.query({
			query,
			format: 'JSONCompactEachRow',
			clickhouse_settings: { output_format_json_quote_64bit_integers: 0 }
		})
		const data = await result.json()

		res.json({ data })
	} catch (err) {
		console.log(err)
		res.status(500).send()
	}
})

app.get('/ping', (req, res) => {
	res.send('ok')
})

app.listen(port, () => {
	console.log(`Server running at port ${port}`)
})
