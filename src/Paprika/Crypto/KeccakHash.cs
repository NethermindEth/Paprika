//  Copyright (c) 2021 Demerzel Solutions Limited
//  This file is part of the Nethermind library.
//
//  The Nethermind library is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  The Nethermind library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//  GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public License
//  along with the Nethermind. If not, see <http://www.gnu.org/licenses/>.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static System.Numerics.BitOperations;

// ReSharper disable InconsistentNaming
namespace Paprika.Crypto;

public sealed class KeccakHash
{
    private const int ROUNDS = 24;

    private static readonly ulong[] RoundConstants =
    {
        0x0000000000000001UL, 0x0000000000008082UL, 0x800000000000808aUL,
        0x8000000080008000UL, 0x000000000000808bUL, 0x0000000080000001UL,
        0x8000000080008081UL, 0x8000000000008009UL, 0x000000000000008aUL,
        0x0000000000000088UL, 0x0000000080008009UL, 0x000000008000000aUL,
        0x000000008000808bUL, 0x800000000000008bUL, 0x8000000000008089UL,
        0x8000000000008003UL, 0x8000000000008002UL, 0x8000000000000080UL,
        0x000000000000800aUL, 0x800000008000000aUL, 0x8000000080008081UL,
        0x8000000000008080UL, 0x0000000080000001UL, 0x8000000080008008UL
    };

    // update the state with given number of rounds
    private static void KeccakF(Span<ulong> st)
    {
        Debug.Assert(st.Length == 25);

        ulong aba, abe, abi, abo, abu;
        ulong aga, age, agi, ago, agu;
        ulong aka, ake, aki, ako, aku;
        ulong ama, ame, ami, amo, amu;
        ulong asa, ase, asi, aso, asu;
        ulong bCa, bCe, bCi, bCo, bCu;
        ulong da, de, di, @do, du;
        ulong eba, ebe, ebi, ebo, ebu;
        ulong ega, ege, egi, ego, egu;
        ulong eka, eke, eki, eko, eku;
        ulong ema, eme, emi, emo, emu;
        ulong esa, ese, esi, eso, esu;

        {
            // Access last element to perform range check once
            // and not for every ascending access
            _ = st[24];
        }
        aba = st[0];
        abe = st[1];
        abi = st[2];
        abo = st[3];
        abu = st[4];
        aga = st[5];
        age = st[6];
        agi = st[7];
        ago = st[8];
        agu = st[9];
        aka = st[10];
        ake = st[11];
        aki = st[12];
        ako = st[13];
        aku = st[14];
        ama = st[15];
        ame = st[16];
        ami = st[17];
        amo = st[18];
        amu = st[19];
        asa = st[20];
        ase = st[21];
        asi = st[22];
        aso = st[23];
        asu = st[24];

        for (int round = 0; round < ROUNDS; round += 2)
        {
            //    prepareTheta
            bCa = aba ^ aga ^ aka ^ ama ^ asa;
            bCe = abe ^ age ^ ake ^ ame ^ ase;
            bCi = abi ^ agi ^ aki ^ ami ^ asi;
            bCo = abo ^ ago ^ ako ^ amo ^ aso;
            bCu = abu ^ agu ^ aku ^ amu ^ asu;

            //thetaRhoPiChiIotaPrepareTheta(round  , A, E)
            da = bCu ^ RotateLeft(bCe, 1);
            de = bCa ^ RotateLeft(bCi, 1);
            di = bCe ^ RotateLeft(bCo, 1);
            @do = bCi ^ RotateLeft(bCu, 1);
            du = bCo ^ RotateLeft(bCa, 1);

            aba ^= da;
            bCa = aba;
            age ^= de;
            bCe = RotateLeft(age, 44);
            aki ^= di;
            bCi = RotateLeft(aki, 43);
            amo ^= @do;
            bCo = RotateLeft(amo, 21);
            asu ^= du;
            bCu = RotateLeft(asu, 14);
            eba = bCa ^ ((~bCe) & bCi);
            eba ^= RoundConstants[round];
            ebe = bCe ^ ((~bCi) & bCo);
            ebi = bCi ^ ((~bCo) & bCu);
            ebo = bCo ^ ((~bCu) & bCa);
            ebu = bCu ^ ((~bCa) & bCe);

            abo ^= @do;
            bCa = RotateLeft(abo, 28);
            agu ^= du;
            bCe = RotateLeft(agu, 20);
            aka ^= da;
            bCi = RotateLeft(aka, 3);
            ame ^= de;
            bCo = RotateLeft(ame, 45);
            asi ^= di;
            bCu = RotateLeft(asi, 61);
            ega = bCa ^ ((~bCe) & bCi);
            ege = bCe ^ ((~bCi) & bCo);
            egi = bCi ^ ((~bCo) & bCu);
            ego = bCo ^ ((~bCu) & bCa);
            egu = bCu ^ ((~bCa) & bCe);

            abe ^= de;
            bCa = RotateLeft(abe, 1);
            agi ^= di;
            bCe = RotateLeft(agi, 6);
            ako ^= @do;
            bCi = RotateLeft(ako, 25);
            amu ^= du;
            bCo = RotateLeft(amu, 8);
            asa ^= da;
            bCu = RotateLeft(asa, 18);
            eka = bCa ^ ((~bCe) & bCi);
            eke = bCe ^ ((~bCi) & bCo);
            eki = bCi ^ ((~bCo) & bCu);
            eko = bCo ^ ((~bCu) & bCa);
            eku = bCu ^ ((~bCa) & bCe);

            abu ^= du;
            bCa = RotateLeft(abu, 27);
            aga ^= da;
            bCe = RotateLeft(aga, 36);
            ake ^= de;
            bCi = RotateLeft(ake, 10);
            ami ^= di;
            bCo = RotateLeft(ami, 15);
            aso ^= @do;
            bCu = RotateLeft(aso, 56);
            ema = bCa ^ ((~bCe) & bCi);
            eme = bCe ^ ((~bCi) & bCo);
            emi = bCi ^ ((~bCo) & bCu);
            emo = bCo ^ ((~bCu) & bCa);
            emu = bCu ^ ((~bCa) & bCe);

            abi ^= di;
            bCa = RotateLeft(abi, 62);
            ago ^= @do;
            bCe = RotateLeft(ago, 55);
            aku ^= du;
            bCi = RotateLeft(aku, 39);
            ama ^= da;
            bCo = RotateLeft(ama, 41);
            ase ^= de;
            bCu = RotateLeft(ase, 2);
            esa = bCa ^ ((~bCe) & bCi);
            ese = bCe ^ ((~bCi) & bCo);
            esi = bCi ^ ((~bCo) & bCu);
            eso = bCo ^ ((~bCu) & bCa);
            esu = bCu ^ ((~bCa) & bCe);

            //    prepareTheta
            bCa = eba ^ ega ^ eka ^ ema ^ esa;
            bCe = ebe ^ ege ^ eke ^ eme ^ ese;
            bCi = ebi ^ egi ^ eki ^ emi ^ esi;
            bCo = ebo ^ ego ^ eko ^ emo ^ eso;
            bCu = ebu ^ egu ^ eku ^ emu ^ esu;

            //thetaRhoPiChiIotaPrepareTheta(round+1, E, A)
            da = bCu ^ RotateLeft(bCe, 1);
            de = bCa ^ RotateLeft(bCi, 1);
            di = bCe ^ RotateLeft(bCo, 1);
            @do = bCi ^ RotateLeft(bCu, 1);
            du = bCo ^ RotateLeft(bCa, 1);

            eba ^= da;
            bCa = eba;
            ege ^= de;
            bCe = RotateLeft(ege, 44);
            eki ^= di;
            bCi = RotateLeft(eki, 43);
            emo ^= @do;
            bCo = RotateLeft(emo, 21);
            esu ^= du;
            bCu = RotateLeft(esu, 14);
            aba = bCa ^ ((~bCe) & bCi);
            aba ^= RoundConstants[round + 1];
            abe = bCe ^ ((~bCi) & bCo);
            abi = bCi ^ ((~bCo) & bCu);
            abo = bCo ^ ((~bCu) & bCa);
            abu = bCu ^ ((~bCa) & bCe);

            ebo ^= @do;
            bCa = RotateLeft(ebo, 28);
            egu ^= du;
            bCe = RotateLeft(egu, 20);
            eka ^= da;
            bCi = RotateLeft(eka, 3);
            eme ^= de;
            bCo = RotateLeft(eme, 45);
            esi ^= di;
            bCu = RotateLeft(esi, 61);
            aga = bCa ^ ((~bCe) & bCi);
            age = bCe ^ ((~bCi) & bCo);
            agi = bCi ^ ((~bCo) & bCu);
            ago = bCo ^ ((~bCu) & bCa);
            agu = bCu ^ ((~bCa) & bCe);

            ebe ^= de;
            bCa = RotateLeft(ebe, 1);
            egi ^= di;
            bCe = RotateLeft(egi, 6);
            eko ^= @do;
            bCi = RotateLeft(eko, 25);
            emu ^= du;
            bCo = RotateLeft(emu, 8);
            esa ^= da;
            bCu = RotateLeft(esa, 18);
            aka = bCa ^ ((~bCe) & bCi);
            ake = bCe ^ ((~bCi) & bCo);
            aki = bCi ^ ((~bCo) & bCu);
            ako = bCo ^ ((~bCu) & bCa);
            aku = bCu ^ ((~bCa) & bCe);

            ebu ^= du;
            bCa = RotateLeft(ebu, 27);
            ega ^= da;
            bCe = RotateLeft(ega, 36);
            eke ^= de;
            bCi = RotateLeft(eke, 10);
            emi ^= di;
            bCo = RotateLeft(emi, 15);
            eso ^= @do;
            bCu = RotateLeft(eso, 56);
            ama = bCa ^ ((~bCe) & bCi);
            ame = bCe ^ ((~bCi) & bCo);
            ami = bCi ^ ((~bCo) & bCu);
            amo = bCo ^ ((~bCu) & bCa);
            amu = bCu ^ ((~bCa) & bCe);

            ebi ^= di;
            bCa = RotateLeft(ebi, 62);
            ego ^= @do;
            bCe = RotateLeft(ego, 55);
            eku ^= du;
            bCi = RotateLeft(eku, 39);
            ema ^= da;
            bCo = RotateLeft(ema, 41);
            ese ^= de;
            bCu = RotateLeft(ese, 2);
            asa = bCa ^ ((~bCe) & bCi);
            ase = bCe ^ ((~bCi) & bCo);
            asi = bCi ^ ((~bCo) & bCu);
            aso = bCo ^ ((~bCu) & bCa);
            asu = bCu ^ ((~bCa) & bCe);
        }

        //copyToState(state, A)
        st[0] = aba;
        st[1] = abe;
        st[2] = abi;
        st[3] = abo;
        st[4] = abu;
        st[5] = aga;
        st[6] = age;
        st[7] = agi;
        st[8] = ago;
        st[9] = agu;
        st[10] = aka;
        st[11] = ake;
        st[12] = aki;
        st[13] = ako;
        st[14] = aku;
        st[15] = ama;
        st[16] = ame;
        st[17] = ami;
        st[18] = amo;
        st[19] = amu;
        st[20] = asa;
        st[21] = ase;
        st[22] = asi;
        st[23] = aso;
        st[24] = asu;
    }

    // compute a Keccak hash (md) of given byte length from "in"
    public static void ComputeHash(ReadOnlySpan<byte> input, out Keccak output)
    {
        const int stateSize = 200;
        const int roundSize = 136;
        const int ulongSize = stateSize / sizeof(ulong);
        const int ulongRounds = roundSize / sizeof(ulong);

        Span<ulong> state = stackalloc ulong[ulongSize];

        int loopLength = input.Length / roundSize * roundSize;
        if (loopLength > 0)
        {
            ReadOnlySpan<ulong> input64 = MemoryMarshal.Cast<byte, ulong>(input[..loopLength]);
            input = input.Slice(loopLength);
            while (input64.Length > 0)
            {
                for (int i = 0; i < ulongRounds; i++)
                {
                    state[i] ^= input64[i];
                }

                input64 = input64[ulongRounds..];
                KeccakF(state);
            }
        }

        Span<byte> temp = stackalloc byte[roundSize];
        input.CopyTo(temp);
        temp[input.Length] = 1;
        temp[roundSize - 1] |= 0x80;

        Span<ulong> tempU64 = MemoryMarshal.Cast<byte, ulong>(temp);
        for (int i = 0; i < tempU64.Length; i++)
        {
            state[i] ^= tempU64[i];
        }

        KeccakF(state);
        output = Unsafe.As<ulong, Keccak>(ref MemoryMarshal.GetReference(state));
    }

    [DoesNotReturn]
    private static void ThrowBadKeccak() => throw new ArgumentException("Bad Keccak use");
}
