const { Client, GatewayIntentBits } = require('discord.js');
const express = require('express');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

const client = new Client({
    intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers]
});

client.once('ready', () => {
    console.log(`🤖 Logged in as ${client.user.tag}`);
});

client.login(process.env.DISCORD_TOKEN);

// Replace these with your actual role IDs from Discord
const ROLE_IDS = {
    Macy: "123456789012345678",
    Mel: "987654321098765432"
};

app.post('/check-roles', async (req, res) => {
    const { discordId } = req.body;

    try {
        const guild = await client.guilds.fetch(process.env.GUILD_ID);
        const member = await guild.members.fetch(discordId);
        const roleIds = member.roles.cache.map(role => role.id);

        res.json({
            Macy: roleIds.includes(ROLE_IDS.Macy),
            Mel: roleIds.includes(ROLE_IDS.Mel)
        });
    } catch (err) {
        console.error("Role check error:", err);
        res.status(500).json({ error: "Could not fetch roles" });
    }
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
    console.log(`🌐 API is running at http://localhost:${port}`);
});