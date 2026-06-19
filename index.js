require('dotenv').config()
const express = require('express')
const http = require('http')
const { Server } = require('socket.io')
const cors = require('cors')
const { createClient } = require('@supabase/supabase-js')

const app = express()
app.use(cors())
app.use(express.json())

const server = http.createServer(app)
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
})

// ── Supabase client ─────────────────────────────────────────────────────────
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
)

// ── In-memory state (rooms) ────────────────────────────────────────────────
// rooms: Map<roomId, RoomState>
// players: Map<socketId, PlayerState>
const rooms = new Map()

const ROOM_PHASES = ['lobby', 'character', 'case', 'clues', 'suspects', 'vote', 'result']
const PHASE_LABELS = {
  lobby: '🟢 Sala de Espera',
  character: '🎭 Escolhendo Papéis',
  case: '📋 Lendo o Caso',
  clues: '🔍 Coletando Pistas',
  suspects: '⚖️ Analisando Suspetos',
  vote: '🗳️ Votando',
  result: '🎉 Resultado',
}

// Criminal character always at index 0 of CHARACTERS
const CHARACTERS = [
  { id: 'criminal', emoji: '🎭', name: 'Criminoso', color: '#c41e3a' },
  { id: 'detective', emoji: '🔍', name: 'Detetive', color: '#c9a84c' },
  { id: 'witnessA', emoji: '👁️', name: 'Testemunha A', color: '#5dade2' },
  { id: 'witnessB', emoji: '🗣️', name: 'Testemunha B', color: '#5dade2' },
  { id: 'family', emoji: '💔', name: 'Família', color: '#9b59b6' },
]

const MAX_PLAYERS = 5
const VOTE_TIMEOUT_MS = 60_000

// ── Helpers ─────────────────────────────────────────────────────────────────
function generateRoomId() {
  return Math.random().toString(36).substring(2, 7).toUpperCase()
}

function generatePlayerId() {
  return 'player_' + Math.random().toString(36).substring(2, 9)
}

async function persistRoom(room) {
  try {
    await supabase.from('mp_rooms').upsert({
      room_id: room.roomId,
      case_id: room.caseId,
      phase: room.phase,
      host_id: room.hostId,
      created_at: room.createdAt,
      players: room.players.map(p => ({ id: p.playerId, name: p.name, charId: p.charId })),
      votes: room.votes,
      criminal_idx: room.criminalIdx,
    })
  } catch (e) {
    console.error('persist error:', e.message)
  }
}

function assignCharacters(players) {
  // Criminal always gets index 0 (first suspect)
  const shuffled = [...players].sort(() => Math.random() - 0.5)
  shuffled.forEach((p, i) => {
    p.charId = CHARACTERS[i % CHARACTERS.length].id
    p.charEmoji = CHARACTERS[i % CHARACTERS.length].emoji
    p.charName = CHARACTERS[i % CHARACTERS.length].name
    p.charColor = CHARACTERS[i % CHARACTERS.length].color
  })
  // Criminal is always the first suspect (index 0)
  const criminal = players.find(p => p.charId === 'criminal')
  if (criminal) {
    // Criminal is at players array position 0 (first suspect)
    criminal.isCriminal = true
  }
  return players
}

function getVisibleInfo(player, allPlayers, caseData) {
  // Each player sees the full case + suspects, but criminal sees themselves
  return {
    ...caseData,
    suspects: caseData.suspects,
    myCharId: player.charId,
    myCharEmoji: player.charEmoji,
    myCharName: player.charName,
    criminalName: allPlayers.find(p => p.isCriminal)?.name || null,
  }
}

// ── REST API ─────────────────────────────────────────────────────────────────
app.get('/health', (req, res) => res.json({ status: 'ok', rooms: rooms.size }))

app.get('/room/:roomId', (req, res) => {
  const room = rooms.get(req.params.roomId.toUpperCase())
  if (!room) return res.status(404).json({ error: 'Sala não encontrada' })
  res.json({
    roomId: room.roomId,
    phase: room.phase,
    players: room.players.map(p => ({ id: p.playerId, name: p.name, charId: p.charId, charEmoji: p.charEmoji, charName: p.charName })),
    playerCount: room.players.length,
    maxPlayers: room.maxPlayers,
  })
})

// ── Socket.io ────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[+] ${socket.id} connected`)

  // ── CREATE ROOM ──────────────────────────────────────────────────────────
  socket.on('create_room', async ({ playerName, caseId }, cb) => {
    const roomId = generateRoomId()
    const playerId = generatePlayerId()
    const player = {
      socketId: socket.id,
      playerId,
      name: playerName.trim().substring(0, 20),
      charId: null,
      charEmoji: null,
      charName: null,
      charColor: null,
      isCriminal: false,
      hasVoted: false,
      votedFor: null,
    }

    const room = {
      roomId,
      caseId: caseId || 1,
      phase: 'lobby',
      hostId: playerId,
      players: [player],
      votes: {}, // { [playerId]: suspectIndex }
      criminalIdx: 0,
      voteStartTime: null,
      voteTimer: null,
      createdAt: new Date().toISOString(),
      maxPlayers: MAX_PLAYERS,
    }

    rooms.set(roomId, room)
    socket.join(roomId)
    socket.data.playerId = playerId
    socket.data.roomId = roomId

    await persistRoom(room)
    console.log(`[+] Room ${roomId} created by ${playerName}`)
    cb({ success: true, roomId, playerId, room: getRoomPublic(room) })
  })

  // ── JOIN ROOM ────────────────────────────────────────────────────────────
  socket.on('join_room', async ({ roomId, playerName }, cb) => {
    const rid = roomId.toUpperCase().trim()
    let room = rooms.get(rid)

    // Try fetching from Supabase if not in memory
    if (!room) {
      const { data } = await supabase.from('mp_rooms').select('*').eq('room_id', rid).single()
      if (!data) return cb({ success: false, error: 'Sala não encontrada.' })
      if (data.phase !== 'lobby') return cb({ success: false, error: 'O jogo já começou nesta sala.' })
      const { data: caseData } = await supabase.from('mp_cases').select('*').eq('id', data.case_id).single()
      room = {
        ...data,
        players: data.players || [],
        votes: data.votes || {},
        caseData,
      }
      rooms.set(rid, room)
    }

    if (room.players.length >= room.maxPlayers) {
      return cb({ success: false, error: 'Sala cheia.' })
    }

    if (room.phase !== 'lobby') {
      return cb({ success: false, error: 'O jogo já começou.' })
    }

    const playerId = generatePlayerId()
    const player = {
      socketId: socket.id,
      playerId,
      name: playerName.trim().substring(0, 20),
      charId: null,
      charEmoji: null,
      charName: null,
      charColor: null,
      isCriminal: false,
      hasVoted: false,
      votedFor: null,
    }

    room.players.push(player)
    socket.join(rid)
    socket.data.playerId = playerId
    socket.data.roomId = rid

    await persistRoom(room)
    io.to(rid).emit('player_joined', { players: room.players.map(p => ({
      id: p.playerId, name: p.name, charId: p.charId,
      charEmoji: p.charEmoji, charName: p.charName,
    })) })

    cb({ success: true, roomId: rid, playerId, room: getRoomPublic(room) })
    console.log(`[+] ${playerName} joined room ${rid}`)
  })

  // ── LEAVE ROOM ────────────────────────────────────────────────────────────
  socket.on('leave_room', async () => {
    await handleLeave(socket)
  })

  // ── START GAME ────────────────────────────────────────────────────────────
  socket.on('start_game', async ({ playerId }, cb) => {
    const roomId = socket.data.roomId
    const room = rooms.get(roomId)
    if (!room) return cb?.({ success: false, error: 'Sala não encontrada.' })
    if (room.hostId !== playerId) return cb?.({ success: false, error: 'Apenas o host pode iniciar.' })
    if (room.players.length < 2) return cb?.({ success: false, error: 'Mínimo 2 jogadores.' })

    // Assign characters
    room.players = assignCharacters(room.players)
    room.phase = 'character'
    room.criminalIdx = room.players.findIndex(p => p.isCriminal)

    // Get case data
    const { data: caseData } = await supabase.from('mp_cases').select('*').eq('id', room.caseId).single()
    room.caseData = caseData || null

    await persistRoom(room)
    io.to(roomId).emit('phase_change', {
      phase: room.phase,
      phaseLabel: PHASE_LABELS[room.phase],
      criminalIdx: room.criminalIdx,
    })

    // Send each player their private info
    room.players.forEach(p => {
      const charInfo = CHARACTERS.find(c => c.id === p.charId)
      io.to(p.socketId).emit('character_assigned', {
        charId: p.charId,
        charEmoji: p.charEmoji,
        charName: p.charName,
        charColor: p.charColor,
        isCriminal: p.isCriminal,
        caseData: room.caseData,
        criminalIdx: room.criminalIdx,
        suspects: caseData?.suspects || [],
      })
    })

    // Auto-advance to case phase after 5s
    setTimeout(() => advancePhase(roomId, 'case'), 5000)
    cb?.({ success: true })
  })

  // ── ADVANCE PHASE (manual) ────────────────────────────────────────────────
  socket.on('advance_phase', async ({ playerId }, cb) => {
    const roomId = socket.data.roomId
    const room = rooms.get(roomId)
    if (!room) return cb?.({ success: false })
    if (room.hostId !== playerId) return cb?.({ success: false, error: 'Apenas o host.' })

    const idx = ROOM_PHASES.indexOf(room.phase)
    const next = ROOM_PHASES[idx + 1]
    if (next) {
      await advancePhase(roomId, next)
      cb?.({ success: true })
    } else {
      cb?.({ success: false })
    }
  })

  // ── VOTE ──────────────────────────────────────────────────────────────────
  socket.on('cast_vote', async ({ playerId, suspectIndex }, cb) => {
    const roomId = socket.data.roomId
    const room = rooms.get(roomId)
    if (!room || room.phase !== 'vote') return cb?.({ success: false, error: 'Votação encerrada.' })

    const player = room.players.find(p => p.playerId === playerId)
    if (!player || player.hasVoted) return cb?.({ success: false, error: 'Já votou.' })

    player.hasVoted = true
    player.votedFor = suspectIndex
    room.votes[playerId] = suspectIndex

    io.to(roomId).emit('vote_update', {
      votedCount: Object.keys(room.votes).length,
      totalPlayers: room.players.length,
    })

    // Check if all voted
    if (Object.keys(room.votes).length === room.players.length) {
      clearTimeout(room.voteTimer)
      await resolveVote(roomId)
    } else {
      // Start timeout
      if (!room.voteTimer) {
        room.voteStartTime = Date.now()
        room.voteTimer = setTimeout(async () => {
          await resolveVote(roomId)
        }, VOTE_TIMEOUT_MS)
      }
    }

    cb?.({ success: true })
  })

  // ── DISCONNECT ─────────────────────────────────────────────────────────────
  socket.on('disconnect', async () => {
    console.log(`[-] ${socket.id} disconnected`)
    await handleLeave(socket)
  })
})

// ── Helpers ──────────────────────────────────────────────────────────────────
async function handleLeave(socket) {
  const { roomId, playerId } = socket.data
  if (!roomId || !playerId) return

  const room = rooms.get(roomId)
  if (!room) return

  room.players = room.players.filter(p => p.playerId !== playerId)
  socket.leave(roomId)

  if (room.players.length === 0) {
    rooms.delete(roomId)
    await supabase.from('mp_rooms').delete().eq('room_id', roomId)
    return
  }

  // Reassign host if host left
  if (room.hostId === playerId) {
    room.hostId = room.players[0].playerId
  }

  await persistRoom(room)
  io.to(roomId).emit('player_left', {
    playerId,
    players: room.players.map(p => ({
      id: p.playerId, name: p.name, charId: p.charId,
      charEmoji: p.charEmoji, charName: p.charName,
    })),
    hostId: room.hostId,
  })
}

async function advancePhase(roomId, newPhase) {
  const room = rooms.get(roomId)
  if (!room) return
  room.phase = newPhase

  // Reset per-phase state
  if (newPhase === 'vote') {
    room.votes = {}
    room.players.forEach(p => { p.hasVoted = false; p.votedFor = null })
    room.voteStartTime = Date.now()
    room.voteTimer = setTimeout(async () => { await resolveVote(roomId) }, VOTE_TIMEOUT_MS)
  }

  await persistRoom(room)
  io.to(roomId).emit('phase_change', {
    phase: newPhase,
    phaseLabel: PHASE_LABELS[newPhase],
    voteEndsAt: newPhase === 'vote' ? Date.now() + VOTE_TIMEOUT_MS : null,
    criminalIdx: room.criminalIdx,
  })
}

async function resolveVote(roomId) {
  const room = rooms.get(roomId)
  if (!room || room.phase !== 'vote') return

  clearTimeout(room.voteTimer)
  room.phase = 'result'

  const voteTally = {}
  room.players.forEach(p => {
    const v = room.votes[p.playerId]
    if (v !== undefined) {
      voteTally[v] = (voteTally[v] || 0) + 1
    }
  })

  const sortedVotes = Object.entries(voteTally).sort((a, b) => b[1] - a[1])
  const topVoteCount = sortedVotes[0] ? sortedVotes[0][1] : 0
  const topSuspects = sortedVotes.filter(([, c]) => c === topVoteCount).map(([idx]) => parseInt(idx))

  const correct = topSuspects.includes(room.criminalIdx)
  const tie = topSuspects.length > 1
  const votesSummary = Object.fromEntries(
    Object.entries(voteTally).map(([idx, cnt]) => [
      room.caseData?.suspects[parseInt(idx)] || `Suspeito ${parseInt(idx) + 1}`,
      cnt
    ])
  )

  await persistRoom(room)
  io.to(roomId).emit('game_result', {
    correct,
    tie,
    criminalIdx: room.criminalIdx,
    criminalName: room.players.find(p => p.isCriminal)?.name || null,
    winnerSuspect: sortedVotes[0] ? parseInt(sortedVotes[0][0]) : null,
    winnerName: room.caseData?.suspects[sortedVotes[0] ? parseInt(sortedVotes[0][0]) : 0] || null,
    voteCount: topVoteCount,
    tieSuspects: topSuspects.map(i => room.caseData?.suspects[i] || `Suspeito ${i + 1}`),
    votesSummary,
    allVotes: room.players.map(p => ({
      name: p.name,
      vote: room.caseData?.suspects[room.votes[p.playerId]] || null,
      isCriminal: p.isCriminal,
    })),
    solution: room.caseData?.solution || null,
    caseData: room.caseData,
  })
}

function getRoomPublic(room) {
  return {
    roomId: room.roomId,
    phase: room.phase,
    phaseLabel: PHASE_LABELS[room.phase],
    playerCount: room.players.length,
    maxPlayers: room.maxPlayers,
    hostId: room.hostId,
    players: room.players.map(p => ({
      id: p.playerId,
      name: p.name,
      charId: p.charId,
      charEmoji: p.charEmoji,
      charName: p.charName,
    })),
    criminalIdx: room.criminalIdx,
  }
}

// ── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001
server.listen(PORT, () => {
  console.log(`Casal Investigador server running on port ${PORT}`)
})
