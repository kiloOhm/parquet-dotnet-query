import type { ColumnInfo, ParquetFileInfo, QueryPredicate } from '@/api/types'

export type QueryIssue = {
  line: number
  severity: 'error' | 'warning'
  message: string
}

export type QueryCompletion = {
  label: string
  detail: string
  insertText: string
  cursorOffset?: number
}

export type QueryCompletionState = {
  start: number
  end: number
  items: QueryCompletion[]
}

type ParsedLine = {
  functionName: string
  arguments: string[]
}

type FunctionSpec = {
  name: string
  operator: string
  minArgs: number
  maxArgs: number
  template: string
  detail: string
  stringOnly?: boolean
}

const FUNCTION_SPECS: FunctionSpec[] = [
  {
    name: 'Eq',
    operator: '==',
    minArgs: 2,
    maxArgs: 2,
    template: 'Eq(Column, "value")',
    detail: 'Pushdown equality filter.',
  },
  {
    name: 'NotEq',
    operator: '!=',
    minArgs: 2,
    maxArgs: 2,
    template: 'NotEq(Column, "value")',
    detail: 'Pushdown inequality filter.',
  },
  {
    name: 'Lt',
    operator: '<',
    minArgs: 2,
    maxArgs: 2,
    template: 'Lt(Column, 100)',
    detail: 'Pushdown less-than filter.',
  },
  {
    name: 'Le',
    operator: '<=',
    minArgs: 2,
    maxArgs: 2,
    template: 'Le(Column, 100)',
    detail: 'Pushdown less-than-or-equal filter.',
  },
  {
    name: 'Gt',
    operator: '>',
    minArgs: 2,
    maxArgs: 2,
    template: 'Gt(Column, 100)',
    detail: 'Pushdown greater-than filter.',
  },
  {
    name: 'Ge',
    operator: '>=',
    minArgs: 2,
    maxArgs: 2,
    template: 'Ge(Column, 100)',
    detail: 'Pushdown greater-than-or-equal filter.',
  },
  {
    name: 'Between',
    operator: 'Between',
    minArgs: 3,
    maxArgs: 3,
    template: 'Between(Column, 10, 20)',
    detail: 'Inclusive lower and upper bounds.',
  },
  {
    name: 'StartsWith',
    operator: 'StartsWith',
    minArgs: 2,
    maxArgs: 2,
    template: 'StartsWith(Column, "prefix")',
    detail: 'Ordered string pushdown using prefix ranges.',
    stringOnly: true,
  },
  {
    name: 'Contains',
    operator: 'Contains',
    minArgs: 2,
    maxArgs: 2,
    template: 'Contains(Column, "substring")',
    detail: 'Residual substring scan after pruning.',
    stringOnly: true,
  },
  {
    name: 'LuceneMatch',
    operator: 'LuceneMatch',
    minArgs: 2,
    maxArgs: 2,
    template: 'LuceneMatch(Column, "term")',
    detail: 'Footer-indexed Lucene term lookup.',
    stringOnly: true,
  },
  {
    name: 'LuceneFuzzy',
    operator: 'LuceneFuzzy',
    minArgs: 2,
    maxArgs: 5,
    template: 'LuceneFuzzy(Column, "term", maxEdits: 1, prefixLength: 0, transpositions: true)',
    detail: 'Footer-indexed fuzzy term lookup.',
    stringOnly: true,
  },
]

const FUNCTION_LOOKUP = new Map(FUNCTION_SPECS.map((spec) => [spec.name.toLowerCase(), spec]))
const FUZZY_OPTION_KEYS = ['maxEdits', 'prefixLength', 'transpositions'] as const

function stripInlineComment(line: string) {
  let inString = false
  let stringChar = '"'
  let escaped = false

  for (let i = 0; i < line.length - 1; i += 1) {
    const char = line[i]

    if (escaped) {
      escaped = false
      continue
    }

    if (char === '\\') {
      escaped = true
      continue
    }

    if (inString) {
      if (char === stringChar) {
        inString = false
      }
      continue
    }

    if (char === '"' || char === '\'') {
      inString = true
      stringChar = char
      continue
    }

    if (char === '/' && line[i + 1] === '/') {
      return line.slice(0, i)
    }
  }

  return line
}

function isInsideQuotedString(text: string) {
  let inString = false
  let stringChar = '"'
  let escaped = false

  for (const char of text) {
    if (escaped) {
      escaped = false
      continue
    }

    if (char === '\\') {
      escaped = true
      continue
    }

    if (!inString && (char === '"' || char === '\'')) {
      inString = true
      stringChar = char
      continue
    }

    if (inString && char === stringChar) {
      inString = false
    }
  }

  return inString
}

function splitArguments(text: string) {
  const args = ['']
  let depth = 0
  let inString = false
  let stringChar = '"'
  let escaped = false

  for (const char of text) {
    if (escaped) {
      args[args.length - 1] += char
      escaped = false
      continue
    }

    if (char === '\\') {
      args[args.length - 1] += char
      escaped = true
      continue
    }

    if (inString) {
      args[args.length - 1] += char
      if (char === stringChar) {
        inString = false
      }
      continue
    }

    if (char === '"' || char === '\'') {
      args[args.length - 1] += char
      inString = true
      stringChar = char
      continue
    }

    if (char === '(') {
      depth += 1
      args[args.length - 1] += char
      continue
    }

    if (char === ')') {
      depth = Math.max(0, depth - 1)
      args[args.length - 1] += char
      continue
    }

    if (char === ',' && depth === 0) {
      args.push('')
      continue
    }

    args[args.length - 1] += char
  }

  return args
}

function decodeQuotedString(value: string) {
  const trimmed = value.trim()

  if (trimmed.length >= 2 && trimmed.startsWith('"') && trimmed.endsWith('"')) {
    return JSON.parse(trimmed) as string
  }

  if (trimmed.length >= 2 && trimmed.startsWith('\'') && trimmed.endsWith('\'')) {
    return trimmed
      .slice(1, -1)
      .replace(/\\\\/g, '\\')
      .replace(/\\'/g, '\'')
  }

  return trimmed
}

function parseColumn(raw: string) {
  const trimmed = raw.trim()
  if (!trimmed) {
    throw new Error('Column name is required.')
  }

  if (
    (trimmed.startsWith('"') && trimmed.endsWith('"')) ||
    (trimmed.startsWith('\'') && trimmed.endsWith('\''))
  ) {
    return decodeQuotedString(trimmed)
  }

  if (!/^[A-Za-z_][A-Za-z0-9_.]*$/.test(trimmed)) {
    throw new Error('Column names must be bare identifiers or quoted strings.')
  }

  return trimmed
}

function parseValue(raw: string) {
  const trimmed = raw.trim()
  if (!trimmed) {
    throw new Error('Value is required.')
  }

  if (
    (trimmed.startsWith('"') && trimmed.endsWith('"')) ||
    (trimmed.startsWith('\'') && trimmed.endsWith('\''))
  ) {
    return decodeQuotedString(trimmed)
  }

  return trimmed
}

function findNamedArgumentSeparator(text: string) {
  let inString = false
  let stringChar = '"'
  let escaped = false

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i]

    if (escaped) {
      escaped = false
      continue
    }

    if (char === '\\') {
      escaped = true
      continue
    }

    if (inString) {
      if (char === stringChar) {
        inString = false
      }
      continue
    }

    if (char === '"' || char === '\'') {
      inString = true
      stringChar = char
      continue
    }

    if (char === ':') {
      return i
    }
  }

  return -1
}

function parseLine(line: string): ParsedLine {
  const match = /^([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)\s*$/.exec(line)
  if (!match) {
    throw new Error('Expected a predicate call like Eq(Column, "value").')
  }

  return {
    functionName: match[1]!,
    arguments: splitArguments(match[2]!).map((arg) => arg.trim()),
  }
}

function escapeCSharpString(value: string) {
  return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')
}

function formatFilePath(path: string) {
  return `@"${path.replace(/"/g, '""')}"`
}

function formatCSharpValue(value: string, clrType?: string) {
  const trimmed = value.trim()
  const type = clrType ?? 'String'

  switch (type) {
    case 'Boolean':
      return trimmed.toLowerCase() === 'true' ? 'true' : 'false'
    case 'Byte':
    case 'SByte':
    case 'Int16':
    case 'UInt16':
    case 'Int32':
    case 'UInt32':
      return trimmed
    case 'Int64':
      return `${trimmed}L`
    case 'UInt64':
      return `${trimmed}UL`
    case 'Single':
      return `${trimmed}f`
    case 'Double':
      return trimmed
    case 'Decimal':
      return `${trimmed}m`
    case 'DateTime':
      return `DateTime.Parse("${escapeCSharpString(value)}")`
    case 'DateTimeOffset':
      return `DateTimeOffset.Parse("${escapeCSharpString(value)}")`
    case 'Guid':
      return `Guid.Parse("${escapeCSharpString(value)}")`
    default:
      return `"${escapeCSharpString(value)}"`
  }
}

export function getQueryTemplates() {
  return FUNCTION_SPECS.map((spec) => spec.template)
}

export function parseQueryText(source: string, columns: ColumnInfo[]) {
  const predicates: QueryPredicate[] = []
  const issues: QueryIssue[] = []
  const columnsByName = new Map(columns.map((column) => [column.name.toLowerCase(), column]))

  source.split(/\r?\n/).forEach((rawLine, index) => {
    const lineNumber = index + 1
    const line = stripInlineComment(rawLine).trim()

    if (!line) {
      return
    }

    try {
      const parsed = parseLine(line)
      const spec = FUNCTION_LOOKUP.get(parsed.functionName.toLowerCase())
      if (!spec) {
        throw new Error(`Unknown predicate '${parsed.functionName}'.`)
      }

      if (parsed.arguments.length < spec.minArgs || parsed.arguments.length > spec.maxArgs) {
        throw new Error(`${spec.name} expects ${spec.minArgs === spec.maxArgs ? spec.minArgs : `${spec.minArgs}-${spec.maxArgs}`} argument(s).`)
      }

      const columnName = parseColumn(parsed.arguments[0]!)
      const column = columnsByName.get(columnName.toLowerCase())
      if (!column) {
        throw new Error(`Unknown column '${columnName}'.`)
      }

      if (spec.stringOnly && column.clrType !== 'String') {
        throw new Error(`${spec.name} can only be used with string columns.`)
      }

      const predicate: QueryPredicate = {
        column: column.name,
        operator: spec.operator,
        value: parseValue(parsed.arguments[1]!),
      }

      if (spec.operator === 'Between') {
        predicate.value2 = parseValue(parsed.arguments[2]!)
      }

      if (spec.operator === 'LuceneFuzzy') {
        const seenOptions = new Set<string>()

        for (const optionArgument of parsed.arguments.slice(2)) {
          const separatorIndex = findNamedArgumentSeparator(optionArgument)
          if (separatorIndex < 0) {
            throw new Error('LuceneFuzzy options must use named arguments such as maxEdits: 1.')
          }

          const optionName = optionArgument.slice(0, separatorIndex).trim()
          const optionValue = optionArgument.slice(separatorIndex + 1).trim()
          if (!FUZZY_OPTION_KEYS.includes(optionName as typeof FUZZY_OPTION_KEYS[number])) {
            throw new Error(`Unknown LuceneFuzzy option '${optionName}'.`)
          }

          if (seenOptions.has(optionName)) {
            throw new Error(`LuceneFuzzy option '${optionName}' was provided more than once.`)
          }

          seenOptions.add(optionName)

          if (optionName === 'transpositions') {
            const normalized = optionValue.toLowerCase()
            if (normalized !== 'true' && normalized !== 'false') {
              throw new Error('transpositions must be true or false.')
            }
            predicate.transpositions = normalized === 'true'
            continue
          }

          const parsedNumber = Number.parseInt(optionValue, 10)
          if (!Number.isFinite(parsedNumber)) {
            throw new Error(`${optionName} must be an integer.`)
          }

          if (optionName === 'maxEdits') {
            predicate.maxEdits = parsedNumber
          } else if (optionName === 'prefixLength') {
            predicate.prefixLength = parsedNumber
          }
        }
      }

      predicates.push(predicate)
    } catch (error) {
      issues.push({
        line: lineNumber,
        severity: 'error',
        message: error instanceof Error ? error.message : String(error),
      })
    }
  })

  return { predicates, issues }
}

export function getQueryCompletions(source: string, cursor: number, columns: ColumnInfo[]): QueryCompletionState | null {
  const lineStart = source.lastIndexOf('\n', Math.max(0, cursor - 1)) + 1
  const beforeCursor = source.slice(lineStart, cursor)

  if (beforeCursor.includes('//') && stripInlineComment(beforeCursor) !== beforeCursor) {
    return null
  }

  if (isInsideQuotedString(beforeCursor)) {
    return null
  }

  const openParenIndex = beforeCursor.indexOf('(')
  if (openParenIndex < 0) {
    const prefixMatch = /[A-Za-z_][A-Za-z0-9_]*$/.exec(beforeCursor)
    const prefix = prefixMatch?.[0] ?? ''
    const items = FUNCTION_SPECS
      .filter((spec) => spec.name.toLowerCase().startsWith(prefix.toLowerCase()))
      .map<QueryCompletion>((spec) => ({
        label: spec.name,
        detail: spec.detail,
        insertText: spec.template,
        cursorOffset: spec.template.indexOf('Column'),
      }))

    return items.length > 0
      ? {
          start: cursor - prefix.length,
          end: cursor,
          items,
        }
      : null
  }

  const functionName = beforeCursor.slice(0, openParenIndex).trim()
  const spec = FUNCTION_LOOKUP.get(functionName.toLowerCase())
  if (!spec) {
    return null
  }

  const argsBeforeCursor = splitArguments(beforeCursor.slice(openParenIndex + 1))
  const currentArgument = argsBeforeCursor[argsBeforeCursor.length - 1] ?? ''
  const argumentIndex = Math.max(0, argsBeforeCursor.length - 1)

  if (argumentIndex === 0) {
    const prefixMatch = /[A-Za-z_][A-Za-z0-9_.]*$/.exec(currentArgument)
    const prefix = prefixMatch?.[0] ?? ''
    const items = columns
      .filter((column) => column.name.toLowerCase().startsWith(prefix.toLowerCase()))
      .map<QueryCompletion>((column) => ({
        label: column.name,
        detail: `${column.dataType} / ${column.clrType}`,
        insertText: column.name,
      }))

    return items.length > 0
      ? {
          start: cursor - prefix.length,
          end: cursor,
          items,
        }
      : null
  }

  if (spec.name === 'LuceneFuzzy' && argumentIndex >= 2) {
    const trimmed = currentArgument.trimStart()
    const keyMatch = /^([A-Za-z_][A-Za-z0-9_]*)\s*:/.exec(trimmed)

    if (keyMatch?.[1] === 'transpositions') {
      const valuePrefix = trimmed.slice(trimmed.indexOf(':') + 1).trimStart()
      const prefixMatch = /[A-Za-z_]*$/.exec(valuePrefix)
      const prefix = prefixMatch?.[0] ?? ''
      const items = ['true', 'false']
        .filter((value) => value.startsWith(prefix.toLowerCase()))
        .map<QueryCompletion>((value) => ({
          label: value,
          detail: 'Boolean option value.',
          insertText: value,
        }))

      return items.length > 0
        ? {
            start: cursor - prefix.length,
            end: cursor,
            items,
          }
        : null
    }

    const prefixMatch = /[A-Za-z_][A-Za-z0-9_]*$/.exec(trimmed)
    const prefix = prefixMatch?.[0] ?? ''
    const existingOptions = new Set(
      argsBeforeCursor
        .slice(2)
        .map((argument) => argument.split(':', 1)[0]?.trim())
        .filter((value): value is string => !!value),
    )

    const items = FUZZY_OPTION_KEYS
      .filter((key) => key.startsWith(prefix))
      .filter((key) => !existingOptions.has(key) || key === prefix)
      .map<QueryCompletion>((key) => {
        if (key === 'maxEdits') {
          return {
            label: key,
            detail: 'Maximum edit distance, usually 1 or 2.',
            insertText: 'maxEdits: 1',
          }
        }

        if (key === 'prefixLength') {
          return {
            label: key,
            detail: 'Leading characters that must match exactly.',
            insertText: 'prefixLength: 0',
          }
        }

        return {
          label: key,
          detail: 'Treat adjacent swaps as a single edit.',
          insertText: 'transpositions: true',
        }
      })

    return items.length > 0
      ? {
          start: cursor - prefix.length,
          end: cursor,
          items,
        }
      : null
  }

  return null
}

const RESIDUAL_OPERATORS = new Set(['Contains', 'EndsWith', 'IsNull', 'IsNotNull'])

export function buildCodeSnippet(fileInfo: ParquetFileInfo, predicates: QueryPredicate[]) {
  const columnsByName = new Map(fileInfo.schema.columns.map((column) => [column.name, column]))
  const usesLucene = predicates.some((predicate) => predicate.operator === 'LuceneMatch' || predicate.operator === 'LuceneFuzzy')
  const pushdownPredicates = predicates.filter((predicate) => !RESIDUAL_OPERATORS.has(predicate.operator))
  const residualPredicates = predicates.filter((predicate) => RESIDUAL_OPERATORS.has(predicate.operator))
  const lines = [
    'var rows = await ParquetQuery',
    `    .FromFile<YourRowType>(${formatFilePath(fileInfo.path)})`,
  ]

  if (usesLucene) {
    lines.push('    .WithLuceneSearch()')
  }

  if (pushdownPredicates.length > 0) {
    lines.push('    .Pushdown(filter => filter')

    for (const predicate of pushdownPredicates) {
      const column = columnsByName.get(predicate.column)
      const value = formatCSharpValue(predicate.value, column?.clrType)

      switch (predicate.operator) {
        case '==':
          lines.push(`        .Eq(row => row.${predicate.column}, ${value})`)
          break
        case '!=':
          lines.push(`        .NotEq(row => row.${predicate.column}, ${value})`)
          break
        case '<':
          lines.push(`        .Lt(row => row.${predicate.column}, ${value})`)
          break
        case '<=':
          lines.push(`        .Le(row => row.${predicate.column}, ${value})`)
          break
        case '>':
          lines.push(`        .Gt(row => row.${predicate.column}, ${value})`)
          break
        case '>=':
          lines.push(`        .Ge(row => row.${predicate.column}, ${value})`)
          break
        case 'Between': {
          const upperBound = formatCSharpValue(predicate.value2 ?? '', column?.clrType)
          lines.push(`        .Between(row => row.${predicate.column}, ${value}, ${upperBound})`)
          break
        }
        case 'StartsWith':
          lines.push(`        .StartsWith(row => row.${predicate.column}, ${value})`)
          break
        case 'LuceneMatch':
          lines.push(`        .LuceneMatch(row => row.${predicate.column}, ${value})`)
          break
        case 'LuceneFuzzy': {
          const maxEdits = predicate.maxEdits ?? 1
          const prefixLength = predicate.prefixLength ?? 0
          const transpositions = predicate.transpositions ?? true
          lines.push(
            `        .LuceneFuzzy(row => row.${predicate.column}, ${value}, maxEdits: ${maxEdits}, prefixLength: ${prefixLength}, transpositions: ${transpositions ? 'true' : 'false'})`,
          )
          break
        }
      }
    }

    lines.push('    )')
  }

  if (residualPredicates.length > 0) {
    const residualExpressions = residualPredicates.map((predicate) => {
      const column = columnsByName.get(predicate.column)
      const value = formatCSharpValue(predicate.value, column?.clrType)

      switch (predicate.operator) {
        case 'Contains':
          return `row.${predicate.column}.Contains(${value}, StringComparison.OrdinalIgnoreCase)`
        case 'EndsWith':
          return `row.${predicate.column}.EndsWith(${value}, StringComparison.OrdinalIgnoreCase)`
        case 'IsNull':
          return `row.${predicate.column} == null`
        case 'IsNotNull':
          return `row.${predicate.column} != null`
        default:
          return `/* ${predicate.operator} */`
      }
    })

    lines.push(`    .Where(row => ${residualExpressions.join(' && ')})`)
  }

  lines.push('    .ToListAsync();')

  return lines.join('\n')
}
